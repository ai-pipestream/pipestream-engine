package ai.pipestream.engine.dlq;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.config.v1.DlqConfig;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.data.v1.DlqMessage;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.metrics.EngineMetrics;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Service for publishing failed documents to Dead Letter Queue (DLQ) topics.
 * <p>
 * When document processing fails after all retry attempts, this service:
 * 1. Creates a DlqMessage with error metadata and the failed stream
 * 2. Publishes to the appropriate DLQ topic (node-specific or global)
 * 3. Tracks reprocess attempts to prevent infinite retry loops for poison messages
 * <p>
 * DLQ topic naming convention:
 * - If node has {@code dlq_config.topic_override} → use that topic
 * - Otherwise → use "pipestream.{cluster}.{node_id}.dlq"
 * <p>
 * The DlqMessage includes:
 * - Original stream (with or without document based on include_payload setting)
 * - Error classification (error type from exception class)
 * - Error message
 * - Failure timestamp
 * - Retry count
 * - Failed node ID
 * - DLQ reprocess count (incremented on each replay from DLQ)
 * - Quarantine status (when max reprocess attempts exceeded)
 */
@ApplicationScoped
public class DlqService {

    private static final Logger LOG = Logger.getLogger(DlqService.class);

    /** Default DLQ topic suffix when no override is configured. */
    private static final String DLQ_SUFFIX = ".dlq";

    /** Quarantine topic suffix for poison messages that exceed max reprocess attempts. */
    private static final String QUARANTINE_SUFFIX = ".quarantine";

    /** Global DLQ topic for nodes without DLQ config or pre-node failures. */
    @ConfigProperty(name = "pipestream.dlq.global-topic", defaultValue = "pipestream.global.dlq")
    String globalDlqTopic;

    /** Current cluster ID for DLQ topic naming. */
    @ConfigProperty(name = "pipestream.cluster.id", defaultValue = "default-cluster")
    String currentClusterId;

    /** Maximum retry attempts from config (used for retry_count in DlqMessage). */
    @ConfigProperty(name = "pipestream.module.retry.max-attempts", defaultValue = "3")
    int maxRetryAttempts;

    /** Maximum times a message can be reprocessed from DLQ before being quarantined. */
    @ConfigProperty(name = "pipestream.dlq.max-reprocess-count", defaultValue = "3")
    int maxDlqReprocessCount;

    /** Emitter for publishing DlqMessage to DLQ topics. Uses dynamic topic routing. */
    @Inject
    @ProtobufChannel("engine-dlq-out")
    ProtobufEmitter<DlqMessage> dlqEmitter;

    /** Metrics service for tracking DLQ publishes. */
    @Inject
    EngineMetrics metrics;

    /**
     * Publishes a failed stream to the Dead Letter Queue.
     * <p>
     * Creates a DlqMessage with error context and publishes to the node's DLQ topic.
     * If the node has DLQ disabled, this method logs and returns without publishing.
     *
     * @param stream The stream that failed processing
     * @param node The node where processing failed
     * @param error The error that caused the failure
     * @return A Uni that completes when the message is published
     */
    public Uni<Void> publishToDlq(PipeStream stream, GraphNode node, Throwable error) {
        DlqConfig dlqConfig = node.getDlqConfig();

        // Check if DLQ is enabled for this node
        if (!dlqConfig.getEnabled()) {
            LOG.debugf("DLQ disabled for node %s, skipping DLQ publish for stream %s",
                    node.getNodeId(), stream.getStreamId());
            return Uni.createFrom().voidItem();
        }

        // Determine the DLQ topic
        String dlqTopic = resolveDlqTopic(node, dlqConfig);

        // Build the DLQ message
        DlqMessage dlqMessage = buildDlqMessage(stream, node, error, dlqConfig);

        LOG.infof("Publishing to DLQ topic '%s' for stream %s (node: %s, error: %s)",
                dlqTopic, stream.getStreamId(), node.getNodeId(), error.getMessage());

        return publishMessage(dlqMessage, dlqTopic, stream.getStreamId());
    }

    /**
     * Publishes to the global DLQ when no node context is available.
     * <p>
     * Used for failures that occur before node processing begins
     * (e.g., intake failures, validation errors).
     *
     * @param stream The stream that failed
     * @param error The error that caused the failure
     * @param context Additional context about where the failure occurred
     * @return A Uni that completes when the message is published
     */
    public Uni<Void> publishToGlobalDlq(PipeStream stream, Throwable error, String context) {
        LOG.infof("Publishing to global DLQ for stream %s (context: %s, error: %s)",
                stream.getStreamId(), context, error.getMessage());

        DlqMessage dlqMessage = buildGlobalDlqMessage(stream, error, context);
        return publishMessage(dlqMessage, globalDlqTopic, stream.getStreamId());
    }

    /**
     * Republishes a failed DLQ message after a reprocess attempt fails.
     * <p>
     * This method is called when a message that was replayed from the DLQ fails again.
     * It increments the reprocess count and checks if the message should be quarantined.
     *
     * @param previousDlqMessage The original DLQ message that was being reprocessed
     * @param node The node where reprocessing failed
     * @param error The new error that occurred
     * @return A Uni that completes when the message is published
     */
    public Uni<Void> republishAfterReprocessFailure(DlqMessage previousDlqMessage,
                                                     GraphNode node, Throwable error) {
        int newReprocessCount = previousDlqMessage.getDlqReprocessCount() + 1;

        // Check if we've exceeded max reprocess attempts
        if (newReprocessCount > maxDlqReprocessCount) {
            return quarantineMessage(previousDlqMessage, node, error);
        }

        DlqConfig dlqConfig = node.getDlqConfig();
        String dlqTopic = resolveDlqTopic(node, dlqConfig);
        Timestamp now = Timestamps.fromMillis(Instant.now().toEpochMilli());

        // Build updated DLQ message with incremented reprocess count
        DlqMessage updatedMessage = previousDlqMessage.toBuilder()
                .setDlqReprocessCount(newReprocessCount)
                .setErrorType(classifyError(error))
                .setErrorMessage(error.getMessage() != null ? error.getMessage() : "Unknown error")
                .setFailedAt(now)
                .setRetryCount(maxRetryAttempts)
                .setFailedNodeId(node.getNodeId())
                .build();

        LOG.warnf("Republishing to DLQ after reprocess failure (attempt %d/%d) for stream %s (node: %s)",
                newReprocessCount, maxDlqReprocessCount,
                previousDlqMessage.getStream().getStreamId(), node.getNodeId());

        return publishMessage(updatedMessage, dlqTopic, previousDlqMessage.getStream().getStreamId());
    }

    /**
     * Checks if a DLQ message has exceeded the maximum reprocess attempts.
     *
     * @param dlqMessage The DLQ message to check
     * @return true if the message should be quarantined, false otherwise
     */
    public boolean shouldQuarantine(DlqMessage dlqMessage) {
        return dlqMessage.getDlqReprocessCount() >= maxDlqReprocessCount;
    }

    /**
     * Checks if a DLQ message is already quarantined.
     *
     * @param dlqMessage The DLQ message to check
     * @return true if the message is quarantined, false otherwise
     */
    public boolean isQuarantined(DlqMessage dlqMessage) {
        return dlqMessage.getQuarantined();
    }

    /**
     * Quarantines a poison message that has exceeded max reprocess attempts.
     * <p>
     * Quarantined messages are published to a separate quarantine topic and marked
     * with the quarantined flag. They require manual intervention to fix.
     *
     * @param dlqMessage The DLQ message to quarantine
     * @param node The node where the failure occurred
     * @param error The error that triggered quarantine
     * @return A Uni that completes when the message is quarantined
     */
    private Uni<Void> quarantineMessage(DlqMessage dlqMessage, GraphNode node, Throwable error) {
        Timestamp now = Timestamps.fromMillis(Instant.now().toEpochMilli());
        String quarantineTopic = resolveQuarantineTopic(node);

        DlqMessage quarantinedMessage = dlqMessage.toBuilder()
                .setDlqReprocessCount(dlqMessage.getDlqReprocessCount() + 1)
                .setQuarantined(true)
                .setQuarantinedAt(now)
                .setErrorType(classifyError(error))
                .setErrorMessage(String.format("QUARANTINED after %d reprocess attempts: %s",
                        maxDlqReprocessCount,
                        error.getMessage() != null ? error.getMessage() : "Unknown error"))
                .setFailedAt(now)
                .build();

        LOG.errorf("QUARANTINING poison message for stream %s after %d failed reprocess attempts. " +
                   "Manual intervention required. Quarantine topic: %s",
                dlqMessage.getStream().getStreamId(), maxDlqReprocessCount, quarantineTopic);

        // Increment quarantine metric
        metrics.incrementDlqQuarantined();

        return publishMessage(quarantinedMessage, quarantineTopic, dlqMessage.getStream().getStreamId());
    }

    /**
     * Resolves the quarantine topic name for a given node.
     *
     * @param node The node configuration
     * @return The quarantine topic name
     */
    private String resolveQuarantineTopic(GraphNode node) {
        return String.format("pipestream.%s.%s%s", currentClusterId, node.getNodeId(), QUARANTINE_SUFFIX);
    }

    /**
     * Resolves the DLQ topic name for a given node.
     *
     * @param node The node configuration
     * @param dlqConfig The DLQ configuration
     * @return The DLQ topic name
     */
    private String resolveDlqTopic(GraphNode node, DlqConfig dlqConfig) {
        // Use topic override if specified
        String topicOverride = dlqConfig.getTopicOverride();
        if (topicOverride != null && !topicOverride.isEmpty()) {
            return topicOverride;
        }

        // Default: pipestream.{cluster}.{node_id}.dlq
        return String.format("pipestream.%s.%s%s", currentClusterId, node.getNodeId(), DLQ_SUFFIX);
    }

    /**
     * Builds a DlqMessage for a node processing failure.
     *
     * @param stream The original stream
     * @param node The node where processing failed
     * @param error The error that occurred
     * @param dlqConfig The DLQ configuration
     * @return A DlqMessage ready for publishing
     */
    private DlqMessage buildDlqMessage(PipeStream stream, GraphNode node,
                                        Throwable error, DlqConfig dlqConfig) {
        Timestamp now = Timestamps.fromMillis(Instant.now().toEpochMilli());

        // Classify error type from exception
        String errorType = classifyError(error);

        // Prepare the stream for DLQ (optionally strip payload)
        PipeStream dlqStream = dlqConfig.getIncludePayload()
                ? stream
                : stripPayload(stream);

        return DlqMessage.newBuilder()
                .setStream(dlqStream)
                .setErrorType(errorType)
                .setErrorMessage(error.getMessage() != null ? error.getMessage() : "Unknown error")
                .setFailedAt(now)
                .setRetryCount(maxRetryAttempts)
                .setFailedNodeId(node.getNodeId())
                .setOriginalTopic(resolveOriginalTopic(node))
                .build();
    }

    /**
     * Builds a DlqMessage for a global (pre-node) failure.
     *
     * @param stream The original stream
     * @param error The error that occurred
     * @param context Additional context about where the failure occurred
     * @return A DlqMessage ready for publishing
     */
    private DlqMessage buildGlobalDlqMessage(PipeStream stream, Throwable error, String context) {
        Timestamp now = Timestamps.fromMillis(Instant.now().toEpochMilli());

        String errorType = classifyError(error);

        return DlqMessage.newBuilder()
                .setStream(stream)
                .setErrorType(errorType)
                .setErrorMessage(String.format("[%s] %s", context,
                        error.getMessage() != null ? error.getMessage() : "Unknown error"))
                .setFailedAt(now)
                .setRetryCount(0) // No retries for pre-node failures
                .setFailedNodeId(context) // Use context as the "node" identifier
                .setOriginalTopic(globalDlqTopic)
                .build();
    }

    /**
     * Classifies the error type from an exception.
     *
     * @param error The exception to classify
     * @return A string representing the error type
     */
    private String classifyError(Throwable error) {
        String className = error.getClass().getSimpleName();

        // Map common exceptions to DLQ error types
        if (className.contains("Timeout") || className.contains("DeadlineExceeded")) {
            return "TIMEOUT";
        }
        if (className.contains("Unavailable") || className.contains("ConnectionRefused")) {
            return "CONNECTION_REFUSED";
        }
        if (className.contains("StatusRuntime")) {
            // Extract gRPC status if possible
            String message = error.getMessage();
            if (message != null) {
                if (message.contains("UNAVAILABLE")) return "UNAVAILABLE";
                if (message.contains("DEADLINE_EXCEEDED")) return "TIMEOUT";
                if (message.contains("RESOURCE_EXHAUSTED")) return "RESOURCE_EXHAUSTED";
                if (message.contains("NOT_FOUND")) return "NOT_FOUND";
                if (message.contains("INVALID_ARGUMENT")) return "INVALID_ARGUMENT";
            }
        }
        if (className.contains("Validation")) {
            return "VALIDATION_ERROR";
        }
        if (className.contains("IllegalState") || className.contains("IllegalArgument")) {
            return "INVALID_STATE";
        }

        // Default: use the exception class name
        return className.toUpperCase().replace("EXCEPTION", "").trim();
    }

    /**
     * Strips the document payload from a stream (when include_payload is false).
     *
     * @param stream The original stream
     * @return A stream without document or document_ref
     */
    private PipeStream stripPayload(PipeStream stream) {
        return stream.toBuilder()
                .clearDocument()
                .clearDocumentRef()
                .build();
    }

    /**
     * Resolves the original topic name for a node (for replay correlation).
     *
     * @param node The node configuration
     * @return The node's input topic
     */
    private String resolveOriginalTopic(GraphNode node) {
        // Use the node's Kafka input topic if available
        String inputTopic = node.getKafkaInputTopic();
        if (inputTopic != null && !inputTopic.isEmpty()) {
            return inputTopic;
        }
        // Default: pipestream.{cluster}.{node_id}
        return String.format("pipestream.%s.%s", currentClusterId, node.getNodeId());
    }

    /**
     * Publishes a DlqMessage to the specified DLQ topic.
     *
     * @param dlqMessage The DLQ message to publish
     * @param topic The DLQ topic name
     * @param streamId The stream ID for key generation
     * @return A Uni that completes when the message is published
     * @throws DlqException.PublishException if publishing fails
     */
    private Uni<Void> publishMessage(DlqMessage dlqMessage, String topic, String streamId) {
        // Generate deterministic key from stream ID
        UUID key;
        try {
            key = UUID.fromString(streamId);
        } catch (IllegalArgumentException e) {
            key = UUID.nameUUIDFromBytes(streamId.getBytes(StandardCharsets.UTF_8));
        }

        OutgoingKafkaRecordMetadata<UUID> metadata = OutgoingKafkaRecordMetadata.<UUID>builder()
                .withKey(key)
                .withTopic(topic)
                .build();

        // ProtobufEmitter.send() is void, wrap in Uni
        try {
            dlqEmitter.send(Message.of(dlqMessage).addMetadata(metadata));
            LOG.debugf("DLQ message sent to topic %s with key %s", topic, key);
            // Record DLQ published metric
            metrics.incrementDlqPublished();
            return Uni.createFrom().voidItem();
        } catch (Exception e) {
            LOG.errorf(e, "Failed to publish DLQ message to topic %s", topic);
            // Record DLQ publish failure metric
            metrics.incrementDlqPublishFailure();
            return Uni.createFrom().failure(new DlqException.PublishException(streamId, topic, e));
        }
    }

    /**
     * Validates that a DLQ message can be reprocessed.
     * <p>
     * Throws appropriate exceptions if the message is already quarantined
     * or should be quarantined based on reprocess count.
     *
     * @param dlqMessage The DLQ message to validate
     * @throws DlqException.AlreadyQuarantinedException if message is already quarantined
     * @throws DlqException.QuarantinedException if message should be quarantined
     */
    public void validateForReprocessing(DlqMessage dlqMessage) {
        String streamId = dlqMessage.hasStream() ? dlqMessage.getStream().getStreamId() : "unknown";

        if (isQuarantined(dlqMessage)) {
            throw new DlqException.AlreadyQuarantinedException(streamId);
        }

        if (shouldQuarantine(dlqMessage)) {
            throw new DlqException.QuarantinedException(
                    streamId,
                    dlqMessage.getDlqReprocessCount(),
                    maxDlqReprocessCount);
        }
    }
}
