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
import org.jboss.logging.Logger;
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

    private static final Logger LOG = Logger.getLogger(TracingContextService.class);

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
     * <p>
     * The trace_id must be a valid W3C trace context format (32 lowercase hex characters).
     * Invalid formats are logged as warnings for debugging purposes.
     *
     * @param stream The PipeStream containing trace metadata
     * @return An OpenTelemetry Context for linking, or Context.root() if no valid trace_id
     */
    public Context createLinkContext(PipeStream stream) {
        if (!stream.hasMetadata() || stream.getMetadata().getTraceId().isEmpty()) {
            return Context.root();
        }

        String traceId = stream.getMetadata().getTraceId();
        String streamId = stream.getStreamId();

        // Validate trace_id format (should be 32 hex characters for W3C trace context)
        if (!isValidW3CTraceId(traceId)) {
            LOG.warnf("Invalid trace_id format for stream %s: '%s' (expected 32 hex characters, got %d chars). " +
                      "Trace linking will be skipped. Ensure upstream services provide W3C-compliant trace IDs.",
                    streamId, sanitizeTraceIdForLog(traceId), traceId.length());
            return Context.root();
        }

        try {
            // Create a SpanContext from the trace_id
            // Use a synthetic span_id since we only have trace_id
            SpanContext linkedContext = SpanContext.create(
                    traceId.toLowerCase(), // W3C requires lowercase
                    "0000000000000000", // Synthetic span_id
                    TraceFlags.getSampled(),
                    TraceState.getDefault()
            );
            return Context.root().with(Span.wrap(linkedContext));
        } catch (Exception e) {
            LOG.warnf(e, "Failed to create SpanContext from trace_id '%s' for stream %s: %s",
                    sanitizeTraceIdForLog(traceId), streamId, e.getMessage());
            return Context.root();
        }
    }

    /**
     * Validates if a trace_id conforms to W3C trace context format.
     * <p>
     * W3C trace context requires trace_id to be exactly 32 lowercase hexadecimal characters.
     *
     * @param traceId The trace_id to validate
     * @return true if valid W3C format, false otherwise
     */
    private boolean isValidW3CTraceId(String traceId) {
        if (traceId == null || traceId.length() != 32) {
            return false;
        }
        // Check for valid hex characters (case-insensitive, will be lowercased)
        return traceId.matches("[0-9a-fA-F]{32}");
    }

    /**
     * Sanitizes a trace_id for safe logging.
     * <p>
     * Truncates very long values and escapes control characters to prevent log injection.
     *
     * @param traceId The trace_id to sanitize
     * @return A safe string for logging
     */
    private String sanitizeTraceIdForLog(String traceId) {
        if (traceId == null) {
            return "<null>";
        }
        // Truncate if too long (shouldn't happen but defensive)
        String safe = traceId.length() > 64 ? traceId.substring(0, 64) + "..." : traceId;
        // Replace any control characters
        return safe.replaceAll("[\\p{Cntrl}]", "?");
    }
}
