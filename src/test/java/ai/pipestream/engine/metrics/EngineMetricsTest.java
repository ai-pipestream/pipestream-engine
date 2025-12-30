package ai.pipestream.engine.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EngineMetrics service.
 */
class EngineMetricsTest {

    private MeterRegistry registry;
    private EngineMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new EngineMetrics(registry);
    }

    @Nested
    @DisplayName("Counter Tests")
    class CounterTests {

        @Test
        @DisplayName("Should increment doc success counter")
        void testIncrementDocSuccess() {
            metrics.incrementDocSuccess();
            metrics.incrementDocSuccess();
            metrics.incrementDocSuccess();

            Counter counter = registry.find("engine.doc.success").counter();
            assertNotNull(counter, "Counter should exist");
            assertThat(counter.count(), is(3.0));
        }

        @Test
        @DisplayName("Should increment doc failure counter")
        void testIncrementDocFailure() {
            metrics.incrementDocFailure();
            metrics.incrementDocFailure();

            Counter counter = registry.find("engine.doc.failure").counter();
            assertNotNull(counter, "Counter should exist");
            assertThat(counter.count(), is(2.0));
        }

        @Test
        @DisplayName("Should increment DLQ published counter")
        void testIncrementDlqPublished() {
            metrics.incrementDlqPublished();

            Counter counter = registry.find("engine.dlq.published").counter();
            assertNotNull(counter, "Counter should exist");
            assertThat(counter.count(), is(1.0));
        }

        @Test
        @DisplayName("Should increment routing dispatched counter")
        void testIncrementRoutingDispatched() {
            metrics.incrementRoutingDispatched();
            metrics.incrementRoutingDispatched();
            metrics.incrementRoutingDispatched();
            metrics.incrementRoutingDispatched();

            Counter counter = registry.find("engine.routing.dispatched").counter();
            assertNotNull(counter, "Counter should exist");
            assertThat(counter.count(), is(4.0));
        }
    }

    @Nested
    @DisplayName("Timer Tests")
    class TimerTests {

        @Test
        @DisplayName("Should record processNode duration")
        void testProcessNodeTimer() {
            Timer.Sample sample = metrics.startProcessNode();

            // Simulate some work
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            metrics.stopProcessNode(sample);

            Timer timer = registry.find("engine.processNode.duration").timer();
            assertNotNull(timer, "Timer should exist");
            assertThat(timer.count(), is(1L));
            assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS), greaterThanOrEqualTo(10.0));
        }

        @Test
        @DisplayName("Should record processNode duration with node tag")
        void testProcessNodeTimerWithTag() {
            Timer.Sample sample = metrics.startProcessNode();
            metrics.stopProcessNode(sample, "test-node-1");

            Timer timer = registry.find("engine.processNode.duration")
                    .tag("node", "test-node-1")
                    .timer();
            assertNotNull(timer, "Tagged timer should exist");
            assertThat(timer.count(), is(1L));
        }

        @Test
        @DisplayName("Should record callModule duration with module tag")
        void testCallModuleTimerWithTag() {
            Timer.Sample sample = metrics.startCallModule();

            // Simulate some work
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            metrics.stopCallModule(sample, "text-chunker");

            Timer timer = registry.find("engine.callModule.duration")
                    .tag("module", "text-chunker")
                    .timer();
            assertNotNull(timer, "Tagged timer should exist");
            assertThat(timer.count(), is(1L));
        }

        @Test
        @DisplayName("Should record dispatch duration")
        void testDispatchTimer() {
            Timer.Sample sample = metrics.startDispatch();
            metrics.stopDispatch(sample);

            Timer timer = registry.find("engine.dispatch.duration").timer();
            assertNotNull(timer, "Timer should exist");
            assertThat(timer.count(), is(1L));
        }

        @Test
        @DisplayName("Should record hydration duration")
        void testHydrationTimer() {
            Timer.Sample sample = metrics.startHydration();
            metrics.stopHydration(sample);

            Timer timer = registry.find("engine.hydration.duration").timer();
            assertNotNull(timer, "Timer should exist");
            assertThat(timer.count(), is(1L));
        }

        @Test
        @DisplayName("Should record duration directly")
        void testRecordDurationDirectly() {
            Duration duration = Duration.ofMillis(100);
            metrics.recordProcessNodeDuration(duration);

            Timer timer = registry.find("engine.processNode.duration").timer();
            assertNotNull(timer, "Timer should exist");
            assertThat(timer.count(), is(1L));
            assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS), closeTo(100.0, 1.0));
        }

        @Test
        @DisplayName("Should record module duration directly with tag")
        void testRecordModuleDurationDirectly() {
            Duration duration = Duration.ofMillis(50);
            metrics.recordCallModuleDuration(duration, "embedder");

            Timer timer = registry.find("engine.callModule.duration")
                    .tag("module", "embedder")
                    .timer();
            assertNotNull(timer, "Timer should exist");
            assertThat(timer.count(), is(1L));
        }
    }

    @Nested
    @DisplayName("Timer Caching Tests")
    class TimerCachingTests {

        @Test
        @DisplayName("Should reuse cached timer for same node")
        void testNodeTimerCaching() {
            // Record multiple samples for the same node
            for (int i = 0; i < 5; i++) {
                Timer.Sample sample = metrics.startProcessNode();
                metrics.stopProcessNode(sample, "cached-node");
            }

            Timer timer = registry.find("engine.processNode.duration")
                    .tag("node", "cached-node")
                    .timer();
            assertNotNull(timer, "Timer should exist");
            assertThat(timer.count(), is(5L));
        }

        @Test
        @DisplayName("Should create separate timers for different modules")
        void testModuleTimerSeparation() {
            Timer.Sample sample1 = metrics.startCallModule();
            metrics.stopCallModule(sample1, "module-a");

            Timer.Sample sample2 = metrics.startCallModule();
            metrics.stopCallModule(sample2, "module-b");

            Timer timerA = registry.find("engine.callModule.duration")
                    .tag("module", "module-a")
                    .timer();
            Timer timerB = registry.find("engine.callModule.duration")
                    .tag("module", "module-b")
                    .timer();

            assertNotNull(timerA, "Timer A should exist");
            assertNotNull(timerB, "Timer B should exist");
            assertThat(timerA.count(), is(1L));
            assertThat(timerB.count(), is(1L));
        }
    }
}
