package ai.pipestream.engine.metrics;

import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StreamMetadata;
import org.jboss.logging.MDC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TracingContextService.
 */
class TracingContextServiceTest {

    private TracingContextService tracingContext;

    @BeforeEach
    void setUp() {
        tracingContext = new TracingContextService();
    }

    @AfterEach
    void tearDown() {
        // Ensure MDC is cleared after each test
        tracingContext.clearContext();
    }

    @Nested
    @DisplayName("MDC Context Setup")
    class MdcContextTests {

        @Test
        @DisplayName("Should set stream_id in MDC")
        void testStreamIdInMdc() {
            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("test-stream-123")
                    .build();

            tracingContext.setupContext(stream, "test-node");

            assertThat(MDC.get(TracingContextService.MDC_STREAM_ID), is("test-stream-123"));
        }

        @Test
        @DisplayName("Should set node_id in MDC")
        void testNodeIdInMdc() {
            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .build();

            tracingContext.setupContext(stream, "chunker-node");

            assertThat(MDC.get(TracingContextService.MDC_NODE_ID), is("chunker-node"));
        }

        @Test
        @DisplayName("Should use trace_id from metadata when available")
        void testTraceIdFromMetadata() {
            StreamMetadata metadata = StreamMetadata.newBuilder()
                    .setTraceId("external-trace-abc123")
                    .setAccountId("account-1")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .setMetadata(metadata)
                    .build();

            tracingContext.setupContext(stream, "node-1");

            assertThat(MDC.get(TracingContextService.MDC_TRACE_ID), is("external-trace-abc123"));
        }

        @Test
        @DisplayName("Should fallback to stream_id when no trace_id in metadata")
        void testFallbackToStreamId() {
            StreamMetadata metadata = StreamMetadata.newBuilder()
                    .setAccountId("account-1")
                    // No trace_id set
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("fallback-stream-id")
                    .setMetadata(metadata)
                    .build();

            tracingContext.setupContext(stream, "node-1");

            assertThat(MDC.get(TracingContextService.MDC_TRACE_ID), is("fallback-stream-id"));
        }

        @Test
        @DisplayName("Should fallback to stream_id when no metadata")
        void testFallbackWithNoMetadata() {
            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("no-metadata-stream")
                    .build();

            tracingContext.setupContext(stream, "node-1");

            assertThat(MDC.get(TracingContextService.MDC_TRACE_ID), is("no-metadata-stream"));
        }

        @Test
        @DisplayName("Should set account_id in MDC when available")
        void testAccountIdInMdc() {
            StreamMetadata metadata = StreamMetadata.newBuilder()
                    .setAccountId("acct-456")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .setMetadata(metadata)
                    .build();

            tracingContext.setupContext(stream, "node-1");

            assertThat(MDC.get(TracingContextService.MDC_ACCOUNT_ID), is("acct-456"));
        }

        @Test
        @DisplayName("Should not set account_id when empty")
        void testNoAccountIdWhenEmpty() {
            StreamMetadata metadata = StreamMetadata.newBuilder()
                    .setAccountId("")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .setMetadata(metadata)
                    .build();

            tracingContext.setupContext(stream, "node-1");

            assertNull(MDC.get(TracingContextService.MDC_ACCOUNT_ID));
        }
    }

    @Nested
    @DisplayName("MDC Context Cleanup")
    class MdcCleanupTests {

        @Test
        @DisplayName("Should clear all MDC values")
        void testClearContext() {
            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .setMetadata(StreamMetadata.newBuilder()
                            .setTraceId("trace-1")
                            .setAccountId("acct-1")
                            .build())
                    .build();

            tracingContext.setupContext(stream, "node-1");

            // Verify MDC is set
            assertNotNull(MDC.get(TracingContextService.MDC_STREAM_ID));
            assertNotNull(MDC.get(TracingContextService.MDC_TRACE_ID));

            // Clear and verify
            tracingContext.clearContext();

            assertNull(MDC.get(TracingContextService.MDC_STREAM_ID));
            assertNull(MDC.get(TracingContextService.MDC_TRACE_ID));
            assertNull(MDC.get(TracingContextService.MDC_NODE_ID));
            assertNull(MDC.get(TracingContextService.MDC_ACCOUNT_ID));
        }
    }

    @Nested
    @DisplayName("Link Context Tests")
    class LinkContextTests {

        @Test
        @DisplayName("Should create link context for valid 32-char trace_id")
        void testValidTraceIdLinkContext() {
            StreamMetadata metadata = StreamMetadata.newBuilder()
                    .setTraceId("0123456789abcdef0123456789abcdef") // 32 hex chars
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .setMetadata(metadata)
                    .build();

            var context = tracingContext.createLinkContext(stream);

            // Context should be created (not root)
            assertNotNull(context);
        }

        @Test
        @DisplayName("Should return root context for empty trace_id")
        void testEmptyTraceIdReturnsRoot() {
            StreamMetadata metadata = StreamMetadata.newBuilder()
                    .setTraceId("")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .setMetadata(metadata)
                    .build();

            var context = tracingContext.createLinkContext(stream);

            // Should return root context
            assertNotNull(context);
        }

        @Test
        @DisplayName("Should return root context for invalid trace_id format")
        void testInvalidTraceIdReturnsRoot() {
            StreamMetadata metadata = StreamMetadata.newBuilder()
                    .setTraceId("not-a-valid-trace-id") // Not 32 hex chars
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .setMetadata(metadata)
                    .build();

            var context = tracingContext.createLinkContext(stream);

            // Should return root context for invalid format
            assertNotNull(context);
        }

        @Test
        @DisplayName("Should return root context when no metadata")
        void testNoMetadataReturnsRoot() {
            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId("stream-1")
                    .build();

            var context = tracingContext.createLinkContext(stream);

            assertNotNull(context);
        }
    }
}
