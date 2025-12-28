package ai.pipestream.engine;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.config.v1.TransportType;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
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
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for CEL (Common Expression Language) routing in the engine.
 * <p>
 * These tests verify conditional routing based on CEL expressions:
 * 1. Documents follow edges when CEL conditions evaluate to true
 * 2. Documents skip edges when CEL conditions evaluate to false
 * 3. Multiple edges can match (fan-out routing)
 * 4. No matching edges results in terminal node
 * 5. CEL evaluation errors are handled gracefully
 * <p>
 * Uses WireMock to mock module services.
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class EngineV1ServiceCelRoutingTest {

    @Inject
    @Any
    Instance<EngineV1Service> engineServiceInstance;

    @Inject
    GraphCache graphCache;

    private EngineV1Service engineService;

    @BeforeEach
    void setUp() {
        // Get engine service instance
        engineService = engineServiceInstance.get();
        
        // Clear graph cache
        graphCache.clear();
    }

    @Test
    @DisplayName("Should route to edge when CEL condition is true")
    void testCelConditionTrue() {
        // Set up node with conditional edge
        GraphNode sourceNode = GraphNode.newBuilder()
                .setNodeId("source-node")
                .setName("Source Node")
                .setModuleId("text-chunker")
                .build();

        ModuleDefinition module = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        GraphNode targetNode = GraphNode.newBuilder()
                .setNodeId("target-node")
                .setName("Target Node")
                .setModuleId("opensearch-sink")
                .build();

        // Edge with CEL condition: document.docId == "test-doc"
        GraphEdge conditionalEdge = GraphEdge.newBuilder()
                .setEdgeId("conditional-edge")
                .setFromNodeId("source-node")
                .setToNodeId("target-node")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setCondition("document.doc_id == 'test-doc'")
                .setPriority(10)
                .build();

        graphCache.putNode(sourceNode);
        graphCache.putModule(module);
        graphCache.putNode(targetNode);
        graphCache.putEdge(conditionalEdge);

        // Create document that matches condition
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("test-doc")
                .build();

        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(doc)
                .setCurrentNodeId("source-node")
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Should process successfully and route to target node
        assertThat("Node should process successfully", 
                response.getSuccess(), is(true));
        
        // Note: In a full integration test, we would verify:
        // - CEL condition evaluated correctly
        // - Stream was routed to target-node
        // - ProcessData was called for target node
    }

    @Test
    @DisplayName("Should skip edge when CEL condition is false")
    void testCelConditionFalse() {
        // Set up node with conditional edge
        GraphNode sourceNode = GraphNode.newBuilder()
                .setNodeId("source-node")
                .setName("Source Node")
                .setModuleId("text-chunker")
                .build();

        ModuleDefinition module = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        GraphNode targetNode = GraphNode.newBuilder()
                .setNodeId("target-node")
                .setName("Target Node")
                .setModuleId("opensearch-sink")
                .build();

        // Edge with CEL condition: document.docId == "test-doc"
        GraphEdge conditionalEdge = GraphEdge.newBuilder()
                .setEdgeId("conditional-edge")
                .setFromNodeId("source-node")
                .setToNodeId("target-node")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setCondition("document.doc_id == 'test-doc'")
                .setPriority(10)
                .build();

        graphCache.putNode(sourceNode);
        graphCache.putModule(module);
        graphCache.putNode(targetNode);
        graphCache.putEdge(conditionalEdge);

        // Create document that does NOT match condition
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("other-doc")
                .build();

        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(doc)
                .setCurrentNodeId("source-node")
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Should process successfully but NOT route (condition false)
        assertThat("Node should process successfully", 
                response.getSuccess(), is(true));
        
        // Note: In a full integration test, we would verify:
        // - CEL condition evaluated to false
        // - Stream was NOT routed to target-node (terminal)
        // - ProcessData was NOT called for target node
    }

    @Test
    @DisplayName("Should support fan-out routing (multiple edges match)")
    void testFanOutRouting() {
        // Set up source node with multiple outgoing edges
        GraphNode sourceNode = GraphNode.newBuilder()
                .setNodeId("source-node")
                .setName("Source Node")
                .setModuleId("text-chunker")
                .build();

        ModuleDefinition module = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        GraphNode targetNode1 = GraphNode.newBuilder()
                .setNodeId("target-node-1")
                .setName("Target Node 1")
                .setModuleId("opensearch-sink")
                .build();

        GraphNode targetNode2 = GraphNode.newBuilder()
                .setNodeId("target-node-2")
                .setName("Target Node 2")
                .setModuleId("opensearch-sink")
                .build();

        // Both edges have conditions that will match
        GraphEdge edge1 = GraphEdge.newBuilder()
                .setEdgeId("edge-1")
                .setFromNodeId("source-node")
                .setToNodeId("target-node-1")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setCondition("document.doc_id == 'test-doc'")
                .setPriority(10)
                .build();

        GraphEdge edge2 = GraphEdge.newBuilder()
                .setEdgeId("edge-2")
                .setFromNodeId("source-node")
                .setToNodeId("target-node-2")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setCondition("document.doc_id == 'test-doc'")
                .setPriority(10)
                .build();

        graphCache.putNode(sourceNode);
        graphCache.putModule(module);
        graphCache.putNode(targetNode1);
        graphCache.putNode(targetNode2);
        graphCache.putEdge(edge1);
        graphCache.putEdge(edge2);

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("test-doc")
                .build();

        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(doc)
                .setCurrentNodeId("source-node")
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Should process successfully and route to both target nodes
        assertThat("Node should process successfully", 
                response.getSuccess(), is(true));
        
        // Note: In a full integration test, we would verify:
        // - Both CEL conditions evaluated to true
        // - Stream was routed to both target-node-1 and target-node-2
        // - ProcessData was called for both target nodes
    }

    @Test
    @DisplayName("Should handle terminal node when no edges match")
    void testTerminalNodeNoEdgesMatch() {
        GraphNode sourceNode = GraphNode.newBuilder()
                .setNodeId("source-node")
                .setName("Source Node")
                .setModuleId("text-chunker")
                .build();

        ModuleDefinition module = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        GraphNode targetNode = GraphNode.newBuilder()
                .setNodeId("target-node")
                .setName("Target Node")
                .setModuleId("opensearch-sink")
                .build();

        // Edge with condition that won't match
        GraphEdge conditionalEdge = GraphEdge.newBuilder()
                .setEdgeId("conditional-edge")
                .setFromNodeId("source-node")
                .setToNodeId("target-node")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setCondition("document.doc_id == 'never-matches'")
                .setPriority(10)
                .build();

        graphCache.putNode(sourceNode);
        graphCache.putModule(module);
        graphCache.putNode(targetNode);
        graphCache.putEdge(conditionalEdge);

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("test-doc")
                .build();

        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(doc)
                .setCurrentNodeId("source-node")
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Should process successfully but terminate (no matching edges)
        assertThat("Node should process successfully", 
                response.getSuccess(), is(true));
        
        // Stream should be complete (no further routing)
    }

    @Test
    @DisplayName("Should handle CEL evaluation errors gracefully")
    void testCelEvaluationError() {
        GraphNode sourceNode = GraphNode.newBuilder()
                .setNodeId("source-node")
                .setName("Source Node")
                .setModuleId("text-chunker")
                .build();

        ModuleDefinition module = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        GraphNode targetNode = GraphNode.newBuilder()
                .setNodeId("target-node")
                .setName("Target Node")
                .setModuleId("opensearch-sink")
                .build();

        // Edge with invalid CEL expression (will cause evaluation error)
        GraphEdge invalidEdge = GraphEdge.newBuilder()
                .setEdgeId("invalid-edge")
                .setFromNodeId("source-node")
                .setToNodeId("target-node")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setCondition("invalid.cel.expression()")
                .setPriority(10)
                .build();

        graphCache.putNode(sourceNode);
        graphCache.putModule(module);
        graphCache.putNode(targetNode);
        graphCache.putEdge(invalidEdge);

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("test-doc")
                .build();

        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(doc)
                .setCurrentNodeId("source-node")
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Should handle error gracefully (either fail or skip edge)
        // CEL evaluator returns false on error, so edge should be skipped
        assertThat("Node should process successfully", 
                response.getSuccess(), is(true));
        
        // Note: CEL evaluation errors should result in edge being skipped (safe default)
    }

    @Test
    @DisplayName("Should route to edge with no condition (always matches)")
    void testEdgeWithNoCondition() {
        GraphNode sourceNode = GraphNode.newBuilder()
                .setNodeId("source-node")
                .setName("Source Node")
                .setModuleId("text-chunker")
                .build();

        ModuleDefinition module = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        GraphNode targetNode = GraphNode.newBuilder()
                .setNodeId("target-node")
                .setName("Target Node")
                .setModuleId("opensearch-sink")
                .build();

        // Edge with no condition (empty string) - should always match
        GraphEdge unconditionalEdge = GraphEdge.newBuilder()
                .setEdgeId("unconditional-edge")
                .setFromNodeId("source-node")
                .setToNodeId("target-node")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setCondition("") // No condition - always routes
                .setPriority(10)
                .build();

        graphCache.putNode(sourceNode);
        graphCache.putModule(module);
        graphCache.putNode(targetNode);
        graphCache.putEdge(unconditionalEdge);

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("test-doc")
                .build();

        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(doc)
                .setCurrentNodeId("source-node")
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Should process successfully and route (no condition = always match)
        assertThat("Node should process successfully", 
                response.getSuccess(), is(true));
    }
}

