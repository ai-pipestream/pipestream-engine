package ai.pipestream.engine.kafka;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.data.v1.PipeStream;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Extracts UUID key from PipeStream for Kafka partitioning.
 * <p>
 * Uses the stream_id to derive a deterministic UUID for partition locality.
 * This ensures all messages for the same stream are routed to the same partition,
 * maintaining message ordering and enabling efficient stream processing.
 * <p>
 * If the stream_id is already a valid UUID, it uses that directly.
 * Otherwise, creates a deterministic UUID using name-based hashing (UUID v5).
 */
@ApplicationScoped
public class PipeStreamKeyExtractor implements UuidKeyExtractor<PipeStream> {

    /**
     * Extracts a UUID key from the given PipeStream for Kafka partitioning.
     * <p>
     * The key is derived deterministically from the stream ID to ensure
     * all messages in the same stream go to the same partition.
     *
     * @param stream The PipeStream to extract the key from
     * @return A UUID key for Kafka partitioning, never null
     */
    @Override
    public UUID extractKey(PipeStream stream) {
        String streamId = stream.getStreamId();
        if (streamId == null || streamId.isBlank()) {
            return UUID.randomUUID();
        }
        
        // Try to parse as UUID first (most common case)
        try {
            return UUID.fromString(streamId);
        } catch (IllegalArgumentException e) {
            // If not a valid UUID, create deterministic UUID from stream_id using UUID v5 (name-based)
            return UUID.nameUUIDFromBytes(streamId.getBytes(StandardCharsets.UTF_8));
        }
    }
}

