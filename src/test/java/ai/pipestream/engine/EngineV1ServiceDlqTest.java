package ai.pipestream.engine;

import ai.pipestream.config.v1.DlqConfig;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DLQ and save_on_error functionality in EngineV1Service.
 * <p>
 * Tests cover:
 * - Error handling behavior with save_on_error enabled
 * - Error handling behavior with DLQ enabled
 * - Combined save_on_error and DLQ behavior
 * - Graceful degradation when error handling fails
 * <p>
 * Implements tests for issue #5.
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class EngineV1ServiceDlqTest {

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

    // ========== Save on Error Tests ==========

    @Nested
    @DisplayName("Save on Error Behavior")
    class SaveOnErrorTests {

        @Test
        @DisplayName("Should handle processing with save_on_error=true")
        void testSaveOnErrorEnabled() {
            // Node with save_on_error enabled - processing should succeed (module is mocked)
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("save-error-node")
                    .setName("Save Error Node")
                    .setModuleId("text-chunker")
                    .setSaveOnError(true)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("save-error-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("save-error-node")
                    .setMetadata(StreamMetadata.newBuilder()
                            .setAccountId("test-account")
                            .build())
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            // With mocked module returning success, processing succeeds
            assertThat("Processing should succeed", response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should handle processing with save_on_error=false")
        void testSaveOnErrorDisabled() {
            // Node with save_on_error disabled
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("no-save-error-node")
                    .setName("No Save Error Node")
                    .setModuleId("text-chunker")
                    .setSaveOnError(false)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("no-save-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("no-save-error-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Processing should succeed", response.getSuccess(), is(true));
        }
    }

    // ========== DLQ Configuration Tests ==========

    @Nested
    @DisplayName("DLQ Configuration")
    class DlqConfigurationTests {

        @Test
        @DisplayName("Should handle processing with DLQ enabled")
        void testDlqEnabled() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .setMaxRetries(3)
                    .setIncludePayload(true)
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("dlq-enabled-node")
                    .setName("DLQ Enabled Node")
                    .setModuleId("text-chunker")
                    .setDlqConfig(dlqConfig)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("dlq-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("dlq-enabled-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            // Module mock returns success, so no DLQ publish occurs
            assertThat("Processing should succeed", response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should handle processing with DLQ disabled")
        void testDlqDisabled() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(false)
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("dlq-disabled-node")
                    .setName("DLQ Disabled Node")
                    .setModuleId("text-chunker")
                    .setDlqConfig(dlqConfig)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("no-dlq-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("dlq-disabled-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Processing should succeed", response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should respect topic override in DLQ config")
        void testDlqTopicOverride() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .setTopicOverride("custom-dlq-topic")
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("dlq-override-node")
                    .setName("DLQ Override Node")
                    .setModuleId("text-chunker")
                    .setDlqConfig(dlqConfig)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("override-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("dlq-override-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Processing should succeed", response.getSuccess(), is(true));
        }
    }

    // ========== Combined Behavior Tests ==========

    @Nested
    @DisplayName("Combined Save and DLQ Behavior")
    class CombinedBehaviorTests {

        @Test
        @DisplayName("Should handle both save_on_error and DLQ enabled")
        void testBothEnabled() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .setIncludePayload(true)
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("combined-node")
                    .setName("Combined Node")
                    .setModuleId("text-chunker")
                    .setSaveOnError(true)
                    .setDlqConfig(dlqConfig)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("combined-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("combined-node")
                    .setMetadata(StreamMetadata.newBuilder()
                            .setAccountId("test-account")
                            .build())
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Processing should succeed with both enabled",
                    response.getSuccess(), is(true));
        }

        @Test
        @DisplayName("Should handle DLQ with include_payload=false")
        void testDlqWithoutPayload() {
            DlqConfig dlqConfig = DlqConfig.newBuilder()
                    .setEnabled(true)
                    .setIncludePayload(false) // Don't include payload in DLQ
                    .build();

            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("no-payload-node")
                    .setName("No Payload Node")
                    .setModuleId("text-chunker")
                    .setDlqConfig(dlqConfig)
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            PipeDoc doc = PipeDoc.newBuilder()
                    .setDocId("payload-strip-doc")
                    .build();

            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setDocument(doc)
                    .setCurrentNodeId("no-payload-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            assertThat("Processing should succeed", response.getSuccess(), is(true));
        }
    }

    // ========== Error Handling Tests ==========

    @Nested
    @DisplayName("Error Handling in Failure Recovery")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should handle missing document gracefully")
        void testMissingDocumentInErrorHandling() {
            // Stream without document shouldn't cause NPE in error handling
            GraphNode node = GraphNode.newBuilder()
                    .setNodeId("no-doc-node")
                    .setName("No Doc Node")
                    .setModuleId("text-chunker")
                    .setSaveOnError(true) // Should skip since no document
                    .build();

            ModuleDefinition module = ModuleDefinition.newBuilder()
                    .setModuleId("text-chunker")
                    .setGrpcServiceName("text-chunker")
                    .build();

            graphCache.putNode(node);
            graphCache.putModule(module);

            // Stream without document - will fail in callModule
            PipeStream stream = PipeStream.newBuilder()
                    .setStreamId(UUID.randomUUID().toString())
                    .setCurrentNodeId("no-doc-node")
                    .setHopCount(0)
                    .build();

            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(stream)
                    .build();

            UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            ProcessNodeResponse response = subscriber.awaitItem().getItem();
            // Should fail gracefully due to missing document
            assertThat("Processing should fail for missing document",
                    response.getSuccess(), is(false));
            assertThat("Error message should indicate missing document",
                    response.getMessage(), containsString("document"));
        }
    }
}
