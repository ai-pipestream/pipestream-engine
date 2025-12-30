package ai.pipestream.engine.dlq;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.data.v1.DlqMessage;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Extracts UUID key from DlqMessage for Kafka partitioning.
 * <p>
 * Uses the stream_id from the embedded stream to derive a deterministic UUID for
 * partition locality. This ensures all DLQ messages for the same stream are routed
 * to the same partition, maintaining message ordering.
 * <p>
 * When no stream_id is available, uses a deterministic fallback based on other
 * message fields to maintain partition consistency for related messages.
 */
@ApplicationScoped
public class DlqMessageKeyExtractor implements UuidKeyExtractor<DlqMessage> {

    /**
     * Deterministic namespace UUID for DLQ messages without a stream_id.
     * Generated from "pipestream.dlq.fallback" using UUID.nameUUIDFromBytes.
     */
    private static final UUID FALLBACK_NAMESPACE = UUID.nameUUIDFromBytes(
            "pipestream.dlq.fallback".getBytes(StandardCharsets.UTF_8));

    /**
     * Extracts a UUID key from the given DlqMessage for Kafka partitioning.
     *
     * @param dlqMessage The DlqMessage to extract the key from
     * @return A UUID key for Kafka partitioning, never null
     */
    @Override
    public UUID extractKey(DlqMessage dlqMessage) {
        // Get stream ID from the embedded stream
        String streamId = dlqMessage.hasStream()
                ? dlqMessage.getStream().getStreamId()
                : null;

        if (streamId == null || streamId.isBlank()) {
            // Use deterministic fallback based on failed_node_id and error_type
            // This ensures related DLQ messages go to the same partition
            return createDeterministicFallbackKey(dlqMessage);
        }

        // Try to parse as UUID first
        try {
            return UUID.fromString(streamId);
        } catch (IllegalArgumentException e) {
            // If not a valid UUID, create deterministic UUID from stream_id
            return UUID.nameUUIDFromBytes(streamId.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Creates a deterministic fallback key when no stream_id is available.
     * <p>
     * Uses failed_node_id and error_type to create a consistent key, so that
     * DLQ messages with the same failure characteristics go to the same partition.
     * If neither is available, falls back to the namespace UUID.
     *
     * @param dlqMessage The DlqMessage to create a key for
     * @return A deterministic UUID based on message characteristics
     */
    private UUID createDeterministicFallbackKey(DlqMessage dlqMessage) {
        String failedNodeId = dlqMessage.getFailedNodeId();
        String errorType = dlqMessage.getErrorType();

        // Build a deterministic string from available fields
        StringBuilder keyBuilder = new StringBuilder();
        if (failedNodeId != null && !failedNodeId.isBlank()) {
            keyBuilder.append(failedNodeId);
        }
        if (errorType != null && !errorType.isBlank()) {
            keyBuilder.append(":").append(errorType);
        }

        if (keyBuilder.isEmpty()) {
            // No identifying information available, use namespace UUID
            // All such messages go to the same partition
            return FALLBACK_NAMESPACE;
        }

        return UUID.nameUUIDFromBytes(keyBuilder.toString().getBytes(StandardCharsets.UTF_8));
    }
}
