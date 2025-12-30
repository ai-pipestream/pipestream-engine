package ai.pipestream.engine.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Centralized metrics for the Pipestream Engine.
 * <p>
 * Provides counters and timers for:
 * - Document processing success/failure rates
 * - DLQ publishing
 * - Module call latency
 * - Routing/dispatch latency
 * <p>
 * All metrics are exported via Prometheus at /q/metrics.
 */
@ApplicationScoped
public class EngineMetrics {

    private final MeterRegistry registry;

    // Counters
    private final Counter docSuccessCounter;
    private final Counter docFailureCounter;
    private final Counter dlqPublishedCounter;
    private final Counter dlqPublishFailureCounter;
    private final Counter dlqQuarantinedCounter;
    private final Counter routingCounter;

    // Timers
    private final Timer processNodeTimer;
    private final Timer callModuleTimer;
    private final Timer dispatchTimer;
    private final Timer hydrationTimer;

    // Per-node timers (cached for performance)
    private final ConcurrentMap<String, Timer> nodeTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> moduleTimers = new ConcurrentHashMap<>();

    @Inject
    public EngineMetrics(MeterRegistry registry) {
        this.registry = registry;

        // Document processing counters
        this.docSuccessCounter = Counter.builder("engine.doc.success")
                .description("Number of documents successfully processed")
                .register(registry);

        this.docFailureCounter = Counter.builder("engine.doc.failure")
                .description("Number of documents that failed processing")
                .register(registry);

        this.dlqPublishedCounter = Counter.builder("engine.dlq.published")
                .description("Number of documents published to DLQ")
                .register(registry);

        this.dlqPublishFailureCounter = Counter.builder("engine.dlq.publish_failure")
                .description("Number of failed attempts to publish to DLQ (network issues, Kafka downtime, etc.)")
                .register(registry);

        this.dlqQuarantinedCounter = Counter.builder("engine.dlq.quarantined")
                .description("Number of poison messages quarantined after exceeding max reprocess attempts")
                .register(registry);

        this.routingCounter = Counter.builder("engine.routing.dispatched")
                .description("Number of routing dispatches to Kafka or next node")
                .register(registry);

        // Timing meters for hot path operations
        this.processNodeTimer = Timer.builder("engine.processNode.duration")
                .description("Time taken to process a node (full pipeline)")
                .register(registry);

        this.callModuleTimer = Timer.builder("engine.callModule.duration")
                .description("Time taken to call a module via gRPC")
                .register(registry);

        this.dispatchTimer = Timer.builder("engine.dispatch.duration")
                .description("Time taken to dispatch to next node/Kafka")
                .register(registry);

        this.hydrationTimer = Timer.builder("engine.hydration.duration")
                .description("Time taken for document hydration")
                .register(registry);
    }

    // ========== Counter Methods ==========

    /**
     * Increments the document success counter.
     */
    public void incrementDocSuccess() {
        docSuccessCounter.increment();
    }

    /**
     * Increments the document failure counter.
     */
    public void incrementDocFailure() {
        docFailureCounter.increment();
    }

    /**
     * Increments the DLQ published counter.
     */
    public void incrementDlqPublished() {
        dlqPublishedCounter.increment();
    }

    /**
     * Increments the DLQ publish failure counter.
     * Called when publishing to DLQ fails (network issues, Kafka downtime, etc.).
     */
    public void incrementDlqPublishFailure() {
        dlqPublishFailureCounter.increment();
    }

    /**
     * Increments the DLQ quarantined counter.
     * Called when a poison message exceeds max reprocess attempts.
     */
    public void incrementDlqQuarantined() {
        dlqQuarantinedCounter.increment();
    }

    /**
     * Increments the routing dispatched counter.
     */
    public void incrementRoutingDispatched() {
        routingCounter.increment();
    }

    // ========== Timer Methods ==========

    /**
     * Creates a timer sample to measure processNode duration.
     * Call {@link Timer.Sample#stop(Timer)} when the operation completes.
     *
     * @return A new timer sample
     */
    public Timer.Sample startProcessNode() {
        return Timer.start(registry);
    }

    /**
     * Records processNode duration.
     *
     * @param sample The timer sample started with {@link #startProcessNode()}
     */
    public void stopProcessNode(Timer.Sample sample) {
        sample.stop(processNodeTimer);
    }

    /**
     * Creates a timer sample to measure callModule duration.
     *
     * @return A new timer sample
     */
    public Timer.Sample startCallModule() {
        return Timer.start(registry);
    }

    /**
     * Records callModule duration.
     *
     * @param sample The timer sample
     */
    public void stopCallModule(Timer.Sample sample) {
        sample.stop(callModuleTimer);
    }

    /**
     * Records callModule duration with module-specific tagging.
     *
     * @param sample   The timer sample
     * @param moduleId The module that was called
     */
    public void stopCallModule(Timer.Sample sample, String moduleId) {
        Timer timer = moduleTimers.computeIfAbsent(moduleId, id ->
                Timer.builder("engine.callModule.duration")
                        .tag("module", id)
                        .description("Time taken to call module: " + id)
                        .register(registry));
        sample.stop(timer);
    }

    /**
     * Creates a timer sample to measure dispatch duration.
     *
     * @return A new timer sample
     */
    public Timer.Sample startDispatch() {
        return Timer.start(registry);
    }

    /**
     * Records dispatch duration.
     *
     * @param sample The timer sample
     */
    public void stopDispatch(Timer.Sample sample) {
        sample.stop(dispatchTimer);
    }

    /**
     * Creates a timer sample to measure hydration duration.
     *
     * @return A new timer sample
     */
    public Timer.Sample startHydration() {
        return Timer.start(registry);
    }

    /**
     * Records hydration duration.
     *
     * @param sample The timer sample
     */
    public void stopHydration(Timer.Sample sample) {
        sample.stop(hydrationTimer);
    }

    /**
     * Records processNode duration with node-specific tagging.
     *
     * @param sample The timer sample
     * @param nodeId The node that was processed
     */
    public void stopProcessNode(Timer.Sample sample, String nodeId) {
        Timer timer = nodeTimers.computeIfAbsent(nodeId, id ->
                Timer.builder("engine.processNode.duration")
                        .tag("node", id)
                        .description("Time taken to process node: " + id)
                        .register(registry));
        sample.stop(timer);
    }

    /**
     * Records a duration directly without using samples.
     * Useful when duration is already calculated.
     *
     * @param duration The duration to record
     */
    public void recordProcessNodeDuration(Duration duration) {
        processNodeTimer.record(duration);
    }

    /**
     * Records a module call duration directly.
     *
     * @param duration The duration to record
     * @param moduleId The module that was called
     */
    public void recordCallModuleDuration(Duration duration, String moduleId) {
        Timer timer = moduleTimers.computeIfAbsent(moduleId, id ->
                Timer.builder("engine.callModule.duration")
                        .tag("module", id)
                        .description("Time taken to call module: " + id)
                        .register(registry));
        timer.record(duration);
    }
}
