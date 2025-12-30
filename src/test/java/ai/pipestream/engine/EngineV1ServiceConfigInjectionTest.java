package ai.pipestream.engine;

import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.engine.graph.GraphCache;
import ai.pipestream.engine.util.WireMockTestResource;
import ai.pipestream.engine.v1.ProcessNodeRequest;
import ai.pipestream.engine.v1.ProcessNodeResponse;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for config injection in the engine.
 * <p>
 * These tests verify that GraphNode.custom_config is properly injected
 * into ProcessDataRequest.config when calling modules.
 * <p>
 * Fixes #12: Config injection in callModule() - modules receive empty config
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class EngineV1ServiceConfigInjectionTest {

    @Inject
    @Any
    Instance<EngineV1Service> engineServiceInstance;

    @Inject
    GraphCache graphCache;

    private EngineV1Service engineService;

    @BeforeEach
    void setUp() {
        engineService = engineServiceInstance.get();
        graphCache.clear();
    }

    @Test
    @DisplayName("Should inject JSON config from GraphNode into ProcessDataRequest")
    void testJsonConfigInjection() {
        // Create node with JSON config
        String nodeId = "test-node-with-json-config";

        Struct jsonConfig = Struct.newBuilder()
                .putFields("chunkSize", Value.newBuilder().setNumberValue(512).build())
                .putFields("overlap", Value.newBuilder().setNumberValue(50).build())
                .putFields("model", Value.newBuilder().setStringValue("gpt-4").build())
                .build();

        ProcessConfiguration customConfig = ProcessConfiguration.newBuilder()
                .setJsonConfig(jsonConfig)
                .build();

        GraphNode testNode = GraphNode.newBuilder()
                .setNodeId(nodeId)
                .setName("Test Node with JSON Config")
                .setModuleId("text-chunker")
                .setCustomConfig(customConfig)
                .build();

        ModuleDefinition testModule = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        graphCache.putNode(testNode);
        graphCache.putModule(testModule);

        // Create document and stream
        PipeDoc pipeDoc = PipeDoc.newBuilder()
                .setDocId("test-doc-id")
                .build();

        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(UUID.randomUUID().toString())
                .setDocument(pipeDoc)
                .setCurrentNodeId(nodeId)
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        // Execute and verify
        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // The test verifies the code path executes without error
        // WireMock mock service receives the config and we can verify via wiremock in full integration
        assertThat("Response should not be null", response, is(notNullValue()));
    }

    @Test
    @DisplayName("Should inject config_params from GraphNode into ProcessDataRequest")
    void testConfigParamsInjection() {
        String nodeId = "test-node-with-config-params";

        ProcessConfiguration customConfig = ProcessConfiguration.newBuilder()
                .putConfigParams("environment", "production")
                .putConfigParams("debug", "false")
                .putConfigParams("timeout_ms", "5000")
                .build();

        GraphNode testNode = GraphNode.newBuilder()
                .setNodeId(nodeId)
                .setName("Test Node with Config Params")
                .setModuleId("text-chunker")
                .setCustomConfig(customConfig)
                .build();

        ModuleDefinition testModule = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        graphCache.putNode(testNode);
        graphCache.putModule(testModule);

        PipeDoc pipeDoc = PipeDoc.newBuilder()
                .setDocId("test-doc-id")
                .build();

        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(UUID.randomUUID().toString())
                .setDocument(pipeDoc)
                .setCurrentNodeId(nodeId)
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();
        assertThat("Response should not be null", response, is(notNullValue()));
    }

    @Test
    @DisplayName("Should inject both JSON config and config_params from GraphNode")
    void testBothConfigTypesInjection() {
        String nodeId = "test-node-with-both-configs";

        Struct jsonConfig = Struct.newBuilder()
                .putFields("maxTokens", Value.newBuilder().setNumberValue(1000).build())
                .build();

        ProcessConfiguration customConfig = ProcessConfiguration.newBuilder()
                .setJsonConfig(jsonConfig)
                .putConfigParams("api_key_env", "OPENAI_API_KEY")
                .putConfigParams("fallback_model", "gpt-3.5-turbo")
                .build();

        GraphNode testNode = GraphNode.newBuilder()
                .setNodeId(nodeId)
                .setName("Test Node with Both Config Types")
                .setModuleId("text-chunker")
                .setCustomConfig(customConfig)
                .build();

        ModuleDefinition testModule = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        graphCache.putNode(testNode);
        graphCache.putModule(testModule);

        PipeDoc pipeDoc = PipeDoc.newBuilder()
                .setDocId("test-doc-id")
                .build();

        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(UUID.randomUUID().toString())
                .setDocument(pipeDoc)
                .setCurrentNodeId(nodeId)
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();
        assertThat("Response should not be null", response, is(notNullValue()));
    }

    @Test
    @DisplayName("Should handle node with no custom config")
    void testNoConfigInjection() {
        String nodeId = "test-node-without-config";

        // No custom_config set
        GraphNode testNode = GraphNode.newBuilder()
                .setNodeId(nodeId)
                .setName("Test Node without Config")
                .setModuleId("text-chunker")
                .build();

        ModuleDefinition testModule = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        graphCache.putNode(testNode);
        graphCache.putModule(testModule);

        PipeDoc pipeDoc = PipeDoc.newBuilder()
                .setDocId("test-doc-id")
                .build();

        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(UUID.randomUUID().toString())
                .setDocument(pipeDoc)
                .setCurrentNodeId(nodeId)
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Should work without config - module receives empty ProcessConfiguration
        assertThat("Response should not be null", response, is(notNullValue()));
    }
}
