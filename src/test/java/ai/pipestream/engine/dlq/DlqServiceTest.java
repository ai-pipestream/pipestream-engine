package ai.pipestream.engine.dlq;

import ai.pipestream.config.v1.DlqConfig;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.data.v1.DlqMessage;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StreamMetadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DlqService.
 * <p>
 * Tests cover:
 * - DlqMessage building logic
 * - Error type classification
 * - Topic resolution
 * - Payload stripping when include_payload is false
 */
class DlqServiceTest {

    private DlqService dlqService;

    @BeforeEach
    void setUp() throws Exception {
        dlqService = new DlqService();

        // Set config properties via reflection (since we're not in a Quarkus test context)
        setField(dlqService, "globalDlqTopic", "pipestream.global.dlq");
        setField(dlqService, "currentClusterId", "test-cluster");
        setField(dlqService, "maxRetryAttempts", 3);
    }

    private void setField(Object obj, String fieldName, Object value) throws Exception {
        var field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    // ========== Error Classification Tests ==========

    @Nested
    @DisplayName("Error Classification")
    class ErrorClassificationTests {

        @Test
        @DisplayName("Should classify timeout errors correctly")
        void testTimeoutClassification() {
            String result = classifyError(new StatusRuntimeException(Status.DEADLINE_EXCEEDED));
            assertThat(result, is("TIMEOUT"));
        }

        @Test
        @DisplayName("Should classify unavailable errors correctly")
        void testUnavailableClassification() {
            String result = classifyError(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("UNAVAILABLE: Connection refused")));
            assertThat(result, is("UNAVAILABLE"));
        }

        @Test
        @DisplayName("Should classify resource exhausted errors correctly")
        void testResourceExhaustedClassification() {
            String result = classifyError(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED.withDescription("RESOURCE_EXHAUSTED: rate limit")));
            assertThat(result, is("RESOURCE_EXHAUSTED"));
        }

        @Test
        @DisplayName("Should classify not found errors correctly")
        void testNotFoundClassification() {
            String result = classifyError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("NOT_FOUND: resource missing")));
            assertThat(result, is("NOT_FOUND"));
        }

        @Test
        @DisplayName("Should classify invalid argument errors correctly")
        void testInvalidArgumentClassification() {
            String result = classifyError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("INVALID_ARGUMENT: bad input")));
            assertThat(result, is("INVALID_ARGUMENT"));
        }

        @Test
        @DisplayName("Should classify IllegalStateException correctly")
        void testIllegalStateClassification() {
            String result = classifyError(new IllegalStateException("Invalid state"));
            assertThat(result, is("INVALID_STATE"));
        }

        @Test
        @DisplayName("Should classify IllegalArgumentException correctly")
        void testIllegalArgumentClassification() {
            String result = classifyError(new IllegalArgumentException("Bad argument"));
            assertThat(result, is("INVALID_STATE"));
        }

        @Test
        @DisplayName("Should classify validation errors correctly")
        void testValidationErrorClassification() {
            String result = classifyError(new RuntimeException("ValidationException"));
            // The class name is RuntimeException, so it will strip EXCEPTION and use RUNTIME
            assertThat(result, is("RUNTIME"));
        }

        @Test
        @DisplayName("Should use class name for unknown errors")
        void testUnknownErrorClassification() {
            String result = classifyError(new NullPointerException("Null pointer"));
            assertThat(result, is("NULLPOINTER"));
        }

        private String classifyError(Throwable error) {
            try {
                Method method = DlqService.class.getDeclaredMethod("classifyError", Throwable.class);
                method.setAccessible(true);
                return (String) method.invoke(dlqService, error);
            } catch (Exception e) {
                throw new RuntimeException("Failed to invoke classifyError", e);
            }
        }
    }

    // ========== Topic Resolution Tests ==========

    @Nested
    @DisplayName("Topic Resolution")
    class TopicResolutionTests {

        @Test
        @DisplayName("Should use topic override when provided")
        void testTopicOverride() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .setTopicOverride("custom-dlq-topic")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("test-node")
                    .setDlqConfig(dlqConfig)
                    .build();

            String topic = resolveDlqTopic(node, dlqConfig);
            assertThat(topic, is("custom-dlq-topic"));
        }

        @Test
        @DisplayName("Should generate default topic when no override")
        void testDefaultTopicGeneration() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("chunker-v1")
                    .setDlqConfig(dlqConfig)
                    .build();

            String topic = resolveDlqTopic(node, dlqConfig);
            assertThat(topic, is("pipestream.test-cluster.chunker-v1.dlq"));
        }

        @Test
        @DisplayName("Should ignore empty topic override")
        void testEmptyTopicOverride() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .setTopicOverride("")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("parser")
                    .setDlqConfig(dlqConfig)
                    .build();

            String topic = resolveDlqTopic(node, dlqConfig);
            assertThat(topic, is("pipestream.test-cluster.parser.dlq"));
        }

        private String resolveDlqTopic(GraphNode node, DlqConfig dlqConfig) {
            try {
                Method method = DlqService.class.getDeclaredMethod("resolveDlqTopic", GraphNode.class, DlqConfig.class);
                method.setAccessible(true);
                return (String) method.invoke(dlqService, node, dlqConfig);
            } catch (Exception e) {
                throw new RuntimeException("Failed to invoke resolveDlqTopic", e);
            }
        }
    }

    // ========== Payload Stripping Tests ==========

    @Nested
    @DisplayName("Payload Stripping")
    class PayloadStrippingTests {

        @Test
        @DisplayName("Should strip document when include_payload is false")
        void testStripDocument() {
            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("test-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .build();

            PipeStream stripped = stripPayload(stream);

            assertFalse(stripped.hasDocument(), "Document should be stripped");
            assertFalse(stripped.hasDocumentRef(), "Document ref should be stripped");
            assertThat(stripped.getStreamId(), is(stream.getStreamId()));
        }

        @Test
        @DisplayName("Should preserve stream ID and metadata when stripping")
        void testPreserveMetadataWhenStripping() {
            StreamMetadata metadata = StreamMetadata.newBuilder()
                    .setAccountId("acct-123")
                    .setDatasourceId("ds-456")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-789")
                    .setMetadata(metadata)
                    .setDocument(PipeDoc.newBuilder().setDocId("doc").build())
                    .setCurrentNodeId("node-1")
                    .setHopCount(5)
                    .build();

            PipeStream stripped = stripPayload(stream);

            assertThat(stripped.getStreamId(), is("stream-789"));
            assertThat(stripped.getMetadata().getAccountId(), is("acct-123"));
            assertThat(stripped.getCurrentNodeId(), is("node-1"));
            assertThat(stripped.getHopCount(), is(5L));
            assertFalse(stripped.hasDocument());
        }

        private PipeStream stripPayload(PipeStream stream) {
            try {
                Method method = DlqService.class.getDeclaredMethod("stripPayload", PipeStream.class);
                method.setAccessible(true);
                return (PipeStream) method.invoke(dlqService, stream);
            } catch (Exception e) {
                throw new RuntimeException("Failed to invoke stripPayload", e);
            }
        }
    }

    // ========== DlqMessage Building Tests ==========

    @Nested
    @DisplayName("DlqMessage Building")
    class DlqMessageBuildingTests {

        @Test
        @DisplayName("Should build DlqMessage with all required fields")
        void testBuildDlqMessage() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .setIncludePayload(true)
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("test-node")
                    .setModuleId("test-module")
                    .setDlqConfig(dlqConfig)
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(PipeDoc.newBuilder().setDocId("doc-1").build())
                    .build();

            RuntimeException error = new RuntimeException("Test error");

            DlqMessage message = buildDlqMessage(stream, node, error, dlqConfig);

            assertThat(message.getErrorType(), is("RUNTIME"));
            assertThat(message.getErrorMessage(), is("Test error"));
            assertThat(message.getFailedNodeId(), is("test-node"));
            assertThat(message.getRetryCount(), is(3)); // maxRetryAttempts
            assertTrue(message.hasFailedAt());
            assertTrue(message.hasStream());
            assertTrue(message.getStream().hasDocument(), "Should include payload");
        }

        @Test
        @DisplayName("Should strip payload when include_payload is false")
        void testBuildDlqMessageWithoutPayload() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .setIncludePayload(false)
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("test-node")
                    .setDlqConfig(dlqConfig)
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-123")
                    .setDocument(PipeDoc.newBuilder().setDocId("doc-1").build())
                    .build();

            DlqMessage message = buildDlqMessage(stream, node, new RuntimeException("Error"), dlqConfig);

            assertFalse(message.getStream().hasDocument(), "Document should be stripped");
        }

        @Test
        @DisplayName("Should resolve original topic from node input topic")
        void testOriginalTopicFromNode() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("test-node")
                    .setKafkaInputTopic("custom-input-topic")
                    .setDlqConfig(dlqConfig)
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .build();

            DlqMessage message = buildDlqMessage(stream, node, new RuntimeException("Error"), dlqConfig);

            assertThat(message.getOriginalTopic(), is("custom-input-topic"));
        }

        @Test
        @DisplayName("Should generate default original topic when not specified")
        void testDefaultOriginalTopic() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("parser-v1")
                    .setDlqConfig(dlqConfig)
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .build();

            DlqMessage message = buildDlqMessage(stream, node, new RuntimeException("Error"), dlqConfig);

            assertThat(message.getOriginalTopic(), is("pipestream.test-cluster.parser-v1"));
        }

        private DlqMessage buildDlqMessage(PipeStream stream, GraphNode node,
                                            Throwable error, DlqConfig dlqConfig) {
            try {
                Method method = DlqService.class.getDeclaredMethod("buildDlqMessage",
                        PipeStream.class, GraphNode.class, Throwable.class, DlqConfig.class);
                method.setAccessible(true);
                return (DlqMessage) method.invoke(dlqService, stream, node, error, dlqConfig);
            } catch (Exception e) {
                throw new RuntimeException("Failed to invoke buildDlqMessage", e);
            }
        }
    }
}
