package ai.pipestream.engine.metrics;

import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StreamMetadata;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.MDC;

/**
 * Service for managing tracing context correlation between PipeStream metadata
 * and OpenTelemetry/MDC contexts.
 * <p>
 * Ensures that:
 * - trace_id from StreamMetadata is injected into MDC for log correlation
 * - OpenTelemetry spans are linked to the stream's trace context
 * - All log entries during stream processing include correlation IDs
 */
@ApplicationScoped
public class TracingContextService {

    // MDC keys for log correlation
    public static final String MDC_TRACE_ID = "traceId";
    public static final String MDC_STREAM_ID = "streamId";
    public static final String MDC_NODE_ID = "nodeId";
    public static final String MDC_ACCOUNT_ID = "accountId";

    /**
     * Sets up tracing context from a PipeStream for the current execution.
     * <p>
     * Extracts trace_id, stream_id, and other metadata from the stream
     * and populates MDC for log correlation. If the stream contains a
     * trace_id, it will be used; otherwise the stream_id serves as the
     * correlation ID.
     *
     * @param stream The PipeStream containing metadata
     * @param nodeId The current node being processed
     */
    public void setupContext(PipeStream stream, String nodeId) {
        String streamId = stream.getStreamId();
        MDC.put(MDC_STREAM_ID, streamId);
        MDC.put(MDC_NODE_ID, nodeId);

        if (stream.hasMetadata()) {
            StreamMetadata metadata = stream.getMetadata();

            // Use trace_id from metadata if available, otherwise use stream_id
            String traceId = metadata.getTraceId();
            if (traceId != null && !traceId.isEmpty()) {
                MDC.put(MDC_TRACE_ID, traceId);
            } else {
                // Fall back to stream_id as correlation ID
                MDC.put(MDC_TRACE_ID, streamId);
            }

            // Add account_id for multi-tenant correlation
            if (!metadata.getAccountId().isEmpty()) {
                MDC.put(MDC_ACCOUNT_ID, metadata.getAccountId());
            }
        } else {
            // No metadata, use stream_id as trace_id
            MDC.put(MDC_TRACE_ID, streamId);
        }
    }

    /**
     * Clears the tracing context from MDC.
     * <p>
     * Should be called when stream processing completes to prevent
     * context leakage between requests.
     */
    public void clearContext() {
        MDC.remove(MDC_TRACE_ID);
        MDC.remove(MDC_STREAM_ID);
        MDC.remove(MDC_NODE_ID);
        MDC.remove(MDC_ACCOUNT_ID);
    }

    /**
     * Executes a Uni with tracing context setup and cleanup.
     * <p>
     * Sets up context before execution and ensures cleanup after
     * completion (success or failure).
     *
     * @param stream The PipeStream for context
     * @param nodeId The current node
     * @param operation The operation to execute
     * @param <T> The result type
     * @return A Uni with context management
     */
    public <T> Uni<T> withContext(PipeStream stream, String nodeId, Uni<T> operation) {
        return Uni.createFrom().deferred(() -> {
            setupContext(stream, nodeId);
            return operation
                    .onTermination().invoke(this::clearContext);
        });
    }

    /**
     * Adds trace attributes to the current OpenTelemetry span.
     * <p>
     * If there's an active span, adds stream_id and node_id as attributes
     * for distributed tracing correlation.
     *
     * @param stream The PipeStream containing metadata
     * @param nodeId The current node being processed
     */
    public void addSpanAttributes(PipeStream stream, String nodeId) {
        Span currentSpan = Span.current();
        if (currentSpan.getSpanContext().isValid()) {
            currentSpan.setAttribute("stream.id", stream.getStreamId());
            currentSpan.setAttribute("node.id", nodeId);

            if (stream.hasMetadata()) {
                StreamMetadata metadata = stream.getMetadata();
                if (!metadata.getAccountId().isEmpty()) {
                    currentSpan.setAttribute("account.id", metadata.getAccountId());
                }
                if (!metadata.getDatasourceId().isEmpty()) {
                    currentSpan.setAttribute("datasource.id", metadata.getDatasourceId());
                }
                if (!metadata.getTraceId().isEmpty()) {
                    currentSpan.setAttribute("pipestream.trace_id", metadata.getTraceId());
                }
            }

            currentSpan.setAttribute("hop.count", stream.getHopCount());
        }
    }

    /**
     * Creates a link context from the stream's trace_id if available.
     * <p>
     * This allows OpenTelemetry spans to be linked to an external trace
     * (e.g., from the original request that created the document).
     *
     * @param stream The PipeStream containing trace metadata
     * @return An OpenTelemetry Context for linking, or Context.root() if no trace_id
     */
    public Context createLinkContext(PipeStream stream) {
        if (!stream.hasMetadata() || stream.getMetadata().getTraceId().isEmpty()) {
            return Context.root();
        }

        String traceId = stream.getMetadata().getTraceId();

        // Validate trace_id format (should be 32 hex characters for W3C trace context)
        if (traceId.length() == 32 && traceId.matches("[0-9a-fA-F]+")) {
            try {
                // Create a SpanContext from the trace_id
                // Use a synthetic span_id since we only have trace_id
                SpanContext linkedContext = SpanContext.create(
                        traceId,
                        "0000000000000000", // Synthetic span_id
                        TraceFlags.getSampled(),
                        TraceState.getDefault()
                );
                return Context.root().with(Span.wrap(linkedContext));
            } catch (Exception e) {
                // Invalid trace_id format, return root context
                return Context.root();
            }
        }

        return Context.root();
    }
}
