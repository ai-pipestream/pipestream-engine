package ai.pipestream.engine.dlq;

import ai.pipestream.data.v1.DlqMessage;
import ai.pipestream.data.v1.PipeStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DlqMessageKeyExtractor.
 * Verifies deterministic key generation for Kafka partitioning.
 */
class DlqMessageKeyExtractorTest {

    private DlqMessageKeyExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new DlqMessageKeyExtractor();
    }

    @Nested
    @DisplayName("Stream ID Key Extraction")
    class StreamIdTests {

        @Test
        @DisplayName("Should extract UUID from valid UUID stream_id")
        void testValidUuidStreamId() {
            UUID expectedUuid = UUID.randomUUID();
            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(expectedUuid.toString())
                    .build();
            DlqMessage dlqMessage = DlqMessage.newBuilder()
                    .setStream(stream)
                    .build();

            UUID result = extractor.extractKey(dlqMessage);

            assertEquals(expectedUuid, result);
        }

        @Test
        @DisplayName("Should create deterministic UUID from non-UUID stream_id")
        void testNonUuidStreamId() {
            String streamId = "my-custom-stream-id-12345";
            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(streamId)
                    .build();
            DlqMessage dlqMessage = DlqMessage.newBuilder()
                    .setStream(stream)
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage);
            UUID result2 = extractor.extractKey(dlqMessage);

            // Should be deterministic
            assertEquals(result1, result2);
            // Should match nameUUIDFromBytes
            assertEquals(UUID.nameUUIDFromBytes(streamId.getBytes(StandardCharsets.UTF_8)), result1);
        }

        @Test
        @DisplayName("Should be consistent across multiple calls for same stream_id")
        void testConsistentForSameStreamId() {
            String streamId = "test-stream-consistency";
            DlqMessage dlqMessage1 = DlqMessage.newBuilder()
                    .setStream(PipeStream.newBuilder().setStreamId(streamId).build())
                    .build();
            DlqMessage dlqMessage2 = DlqMessage.newBuilder()
                    .setStream(PipeStream.newBuilder().setStreamId(streamId).build())
                    .setErrorType("DIFFERENT_ERROR")
                    .setFailedNodeId("different-node")
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage1);
            UUID result2 = extractor.extractKey(dlqMessage2);

            // Same stream_id should produce same key regardless of other fields
            assertEquals(result1, result2);
        }
    }

    @Nested
    @DisplayName("Fallback Key Extraction")
    class FallbackTests {

        @Test
        @DisplayName("Should use deterministic fallback when stream is missing")
        void testNoStream() {
            DlqMessage dlqMessage = DlqMessage.newBuilder()
                    .setFailedNodeId("node-1")
                    .setErrorType("TIMEOUT")
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage);
            UUID result2 = extractor.extractKey(dlqMessage);

            // Should be deterministic, not random
            assertEquals(result1, result2);
        }

        @Test
        @DisplayName("Should use deterministic fallback when stream_id is empty")
        void testEmptyStreamId() {
            DlqMessage dlqMessage = DlqMessage.newBuilder()
                    .setStream(PipeStream.newBuilder().setStreamId("").build())
                    .setFailedNodeId("chunker-node")
                    .setErrorType("CONNECTION_REFUSED")
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage);
            UUID result2 = extractor.extractKey(dlqMessage);

            assertEquals(result1, result2);
        }

        @Test
        @DisplayName("Should use deterministic fallback when stream_id is blank")
        void testBlankStreamId() {
            DlqMessage dlqMessage = DlqMessage.newBuilder()
                    .setStream(PipeStream.newBuilder().setStreamId("   ").build())
                    .setFailedNodeId("embedder-node")
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage);
            UUID result2 = extractor.extractKey(dlqMessage);

            assertEquals(result1, result2);
        }

        @Test
        @DisplayName("Should generate same key for same failed_node_id and error_type")
        void testSameFallbackKeyForSameFields() {
            DlqMessage dlqMessage1 = DlqMessage.newBuilder()
                    .setFailedNodeId("node-alpha")
                    .setErrorType("TIMEOUT")
                    .build();

            DlqMessage dlqMessage2 = DlqMessage.newBuilder()
                    .setFailedNodeId("node-alpha")
                    .setErrorType("TIMEOUT")
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage1);
            UUID result2 = extractor.extractKey(dlqMessage2);

            assertEquals(result1, result2);
        }

        @Test
        @DisplayName("Should generate different keys for different failed_node_id")
        void testDifferentKeysForDifferentNodes() {
            DlqMessage dlqMessage1 = DlqMessage.newBuilder()
                    .setFailedNodeId("node-1")
                    .setErrorType("TIMEOUT")
                    .build();

            DlqMessage dlqMessage2 = DlqMessage.newBuilder()
                    .setFailedNodeId("node-2")
                    .setErrorType("TIMEOUT")
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage1);
            UUID result2 = extractor.extractKey(dlqMessage2);

            assertNotEquals(result1, result2);
        }

        @Test
        @DisplayName("Should generate different keys for different error_type")
        void testDifferentKeysForDifferentErrors() {
            DlqMessage dlqMessage1 = DlqMessage.newBuilder()
                    .setFailedNodeId("same-node")
                    .setErrorType("TIMEOUT")
                    .build();

            DlqMessage dlqMessage2 = DlqMessage.newBuilder()
                    .setFailedNodeId("same-node")
                    .setErrorType("CONNECTION_REFUSED")
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage1);
            UUID result2 = extractor.extractKey(dlqMessage2);

            assertNotEquals(result1, result2);
        }

        @Test
        @DisplayName("Should use namespace UUID when no identifying fields available")
        void testNamespaceUuidWhenNoFields() {
            DlqMessage dlqMessage1 = DlqMessage.newBuilder().build();
            DlqMessage dlqMessage2 = DlqMessage.newBuilder().build();

            UUID result1 = extractor.extractKey(dlqMessage1);
            UUID result2 = extractor.extractKey(dlqMessage2);

            // Both should get the same fallback namespace UUID
            assertEquals(result1, result2);
            // Verify it's the expected namespace UUID
            UUID expectedNamespace = UUID.nameUUIDFromBytes(
                    "pipestream.dlq.fallback".getBytes(StandardCharsets.UTF_8));
            assertEquals(expectedNamespace, result1);
        }

        @Test
        @DisplayName("Should use failed_node_id alone when error_type is missing")
        void testNodeIdOnlyFallback() {
            DlqMessage dlqMessage = DlqMessage.newBuilder()
                    .setFailedNodeId("lonely-node")
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage);
            UUID result2 = extractor.extractKey(dlqMessage);

            assertEquals(result1, result2);
            // Verify it's based on the node ID
            UUID expected = UUID.nameUUIDFromBytes("lonely-node".getBytes(StandardCharsets.UTF_8));
            assertEquals(expected, result1);
        }

        @Test
        @DisplayName("Should use error_type alone when failed_node_id is missing")
        void testErrorTypeOnlyFallback() {
            DlqMessage dlqMessage = DlqMessage.newBuilder()
                    .setErrorType("VALIDATION_ERROR")
                    .build();

            UUID result1 = extractor.extractKey(dlqMessage);
            UUID result2 = extractor.extractKey(dlqMessage);

            assertEquals(result1, result2);
            // Verify it's based on error type (with colon prefix)
            UUID expected = UUID.nameUUIDFromBytes(":VALIDATION_ERROR".getBytes(StandardCharsets.UTF_8));
            assertEquals(expected, result1);
        }
    }

    @Nested
    @DisplayName("Never Returns Random UUID")
    class NoRandomUuidTests {

        @Test
        @DisplayName("Same empty message should always produce same key")
        void testEmptyMessageDeterministic() {
            DlqMessage emptyMessage = DlqMessage.getDefaultInstance();

            // Generate 100 keys and verify they're all the same
            UUID firstKey = extractor.extractKey(emptyMessage);
            for (int i = 0; i < 100; i++) {
                UUID key = extractor.extractKey(emptyMessage);
                assertEquals(firstKey, key, "Key should be deterministic, not random");
            }
        }

        @Test
        @DisplayName("Message with only stream but empty stream_id should be deterministic")
        void testEmptyStreamIdDeterministic() {
            DlqMessage message = DlqMessage.newBuilder()
                    .setStream(PipeStream.getDefaultInstance())
                    .build();

            UUID firstKey = extractor.extractKey(message);
            for (int i = 0; i < 100; i++) {
                UUID key = extractor.extractKey(message);
                assertEquals(firstKey, key, "Key should be deterministic, not random");
            }
        }
    }
}
