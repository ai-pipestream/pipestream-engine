package ai.pipestream.engine;

import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StepExecutionRecord;
import ai.pipestream.data.v1.StreamMetadata;
import ai.pipestream.engine.graph.GraphCache;
import ai.pipestream.engine.util.WireMockTestResource;
import ai.pipestream.engine.v1.ProcessNodeRequest;
import ai.pipestream.engine.v1.ProcessNodeResponse;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests for enhanced step execution metadata (Issue #2).
 * <p>
 * Verifies that StepExecutionRecord captures:
 * - service_instance_id: Pod name/IP from configuration
 * - start_time: When step processing began
 * - end_time: When step processing ended
 * - Duration can be calculated from end_time - start_time
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class EngineV1ServiceStepMetadataTest {

    @Inject
    @Any
    Instance<EngineV1Service> engineServiceInstance;

    @Inject
    GraphCache graphCache;

    @ConfigProperty(name = "pipestream.engine.instance-id", defaultValue = "unknown")
    String expectedInstanceId;

    private EngineV1Service engineService;

    @BeforeEach
    void setUp() {
        engineService = engineServiceInstance.get();
        graphCache.clear();

        // Set up test node
        GraphNode testNode = GraphNode.newBuilder()
                .setNodeId("metadata-test-node")
                .setName("Metadata Test Node")
                .setModuleId("text-chunker")
                .build();

        ModuleDefinition testModule = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        graphCache.putNode(testNode);
        graphCache.putModule(testModule);
    }

    @Test
    @DisplayName("StepExecutionRecord should contain service_instance_id")
    void testServiceInstanceIdCaptured() {
        PipeStream stream = createTestStream();
        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();
        assertThat("Response should be successful", response.getSuccess(), is(true));

        // Get the history from the updated stream
        StreamMetadata metadata = response.getUpdatedStream().getMetadata();
        assertThat("History should not be empty", metadata.getHistoryCount(), greaterThan(0));

        StepExecutionRecord record = metadata.getHistory(metadata.getHistoryCount() - 1);
        assertThat("service_instance_id should be set",
                record.getServiceInstanceId(), is(not(emptyOrNullString())));
        assertThat("service_instance_id should match config",
                record.getServiceInstanceId(), is(expectedInstanceId));
    }

    @Test
    @DisplayName("StepExecutionRecord should contain start_time")
    void testStartTimeCaptured() {
        PipeStream stream = createTestStream();
        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();
        assertThat("Response should be successful", response.getSuccess(), is(true));

        StreamMetadata metadata = response.getUpdatedStream().getMetadata();
        StepExecutionRecord record = metadata.getHistory(metadata.getHistoryCount() - 1);

        assertThat("start_time should be set", record.hasStartTime(), is(true));
        assertThat("start_time seconds should be positive",
                record.getStartTime().getSeconds(), greaterThan(0L));
    }

    @Test
    @DisplayName("StepExecutionRecord should contain end_time")
    void testEndTimeCaptured() {
        PipeStream stream = createTestStream();
        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();
        assertThat("Response should be successful", response.getSuccess(), is(true));

        StreamMetadata metadata = response.getUpdatedStream().getMetadata();
        StepExecutionRecord record = metadata.getHistory(metadata.getHistoryCount() - 1);

        assertThat("end_time should be set", record.hasEndTime(), is(true));
        assertThat("end_time seconds should be positive",
                record.getEndTime().getSeconds(), greaterThan(0L));
    }

    @Test
    @DisplayName("end_time should be >= start_time (duration is non-negative)")
    void testDurationIsNonNegative() {
        PipeStream stream = createTestStream();
        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();
        assertThat("Response should be successful", response.getSuccess(), is(true));

        StreamMetadata metadata = response.getUpdatedStream().getMetadata();
        StepExecutionRecord record = metadata.getHistory(metadata.getHistoryCount() - 1);

        // Calculate duration in milliseconds
        long startMs = record.getStartTime().getSeconds() * 1000 + record.getStartTime().getNanos() / 1_000_000;
        long endMs = record.getEndTime().getSeconds() * 1000 + record.getEndTime().getNanos() / 1_000_000;
        long durationMs = endMs - startMs;

        assertThat("Duration should be non-negative", durationMs, greaterThanOrEqualTo(0L));
    }

    @Test
    @DisplayName("StepExecutionRecord should have correct step_name and hop_number")
    void testStepNameAndHopNumber() {
        PipeStream stream = createTestStream();
        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();
        assertThat("Response should be successful", response.getSuccess(), is(true));

        StreamMetadata metadata = response.getUpdatedStream().getMetadata();
        StepExecutionRecord record = metadata.getHistory(metadata.getHistoryCount() - 1);

        assertThat("step_name should match node name",
                record.getStepName(), is("Metadata Test Node"));
        assertThat("hop_number should be 1 (first hop)",
                record.getHopNumber(), is(1L));
        assertThat("status should be SUCCESS",
                record.getStatus(), is("SUCCESS"));
    }

    @Test
    @DisplayName("Multiple hops should accumulate history with correct hop numbers")
    void testMultipleHopsAccumulateHistory() {
        // First hop
        PipeStream stream = createTestStream();
        ProcessNodeRequest request1 = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber1 = engineService.processNode(request1)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response1 = subscriber1.awaitItem().getItem();
        assertThat("First response should be successful", response1.getSuccess(), is(true));

        // Second hop - use the updated stream
        PipeStream updatedStream = response1.getUpdatedStream().toBuilder()
                .setCurrentNodeId("metadata-test-node")
                .build();
        ProcessNodeRequest request2 = ProcessNodeRequest.newBuilder()
                .setStream(updatedStream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber2 = engineService.processNode(request2)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response2 = subscriber2.awaitItem().getItem();
        assertThat("Second response should be successful", response2.getSuccess(), is(true));

        // Verify history has 2 entries
        StreamMetadata metadata = response2.getUpdatedStream().getMetadata();
        assertThat("History should have 2 entries", metadata.getHistoryCount(), is(2));

        // Verify hop numbers
        assertThat("First hop number should be 1",
                metadata.getHistory(0).getHopNumber(), is(1L));
        assertThat("Second hop number should be 2",
                metadata.getHistory(1).getHopNumber(), is(2L));

        // Both should have instance ID
        assertThat("First record should have instance ID",
                metadata.getHistory(0).getServiceInstanceId(), is(not(emptyOrNullString())));
        assertThat("Second record should have instance ID",
                metadata.getHistory(1).getServiceInstanceId(), is(not(emptyOrNullString())));
    }

    private PipeStream createTestStream() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("test-doc-" + UUID.randomUUID())
                .build();

        return PipeStream.newBuilder()
                .setStreamId(UUID.randomUUID().toString())
                .setDocument(doc)
                .setCurrentNodeId("metadata-test-node")
                .setHopCount(0)
                .setMetadata(StreamMetadata.newBuilder().build())
                .build();
    }
}
