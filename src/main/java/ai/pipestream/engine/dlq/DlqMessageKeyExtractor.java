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
 */
@ApplicationScoped
public class DlqMessageKeyExtractor implements UuidKeyExtractor<DlqMessage> {

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
            return UUID.randomUUID();
        }

        // Try to parse as UUID first
        try {
            return UUID.fromString(streamId);
        } catch (IllegalArgumentException e) {
            // If not a valid UUID, create deterministic UUID from stream_id
            return UUID.nameUUIDFromBytes(streamId.getBytes(StandardCharsets.UTF_8));
        }
    }
}
