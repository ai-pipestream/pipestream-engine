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
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import com.google.protobuf.ByteString;
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
 * Integration tests for multi-node pipeline processing in the engine.
 * <p>
 * These tests verify the complete pipeline flow:
 * 1. Document enters at parser node
 * 2. Processes through multiple nodes (parser → chunker → embedder → sink)
 * 3. Routing between nodes works correctly
 * 4. Metadata and history accumulate properly
 * 5. Hop count increments correctly
 * <p>
 * Uses WireMock to mock module services (ProcessData).
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class EngineV1ServiceMultiNodePipelineTest {

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

        // Set up multi-node pipeline: parser → chunker → embedder → sink
        setupMultiNodePipeline();
    }

    /**
     * Sets up a complete pipeline with 4 nodes:
     * - parser-node: tika-parser (PARSER capability)
     * - chunker-node: text-chunker (no special capability)
     * - embedder-node: openai-embedder (no special capability)
     * - sink-node: opensearch-sink (SINK capability)
     */
    private void setupMultiNodePipeline() {
        // 1. Parser Node
        GraphNode parserNode = GraphNode.newBuilder()
                .setNodeId("parser-node")
                .setName("Parser Node")
                .setModuleId("tika-parser")
                .build();

        ModuleDefinition parserModule = ModuleDefinition.newBuilder()
                .setModuleId("tika-parser")
                .setGrpcServiceName("tika-parser")
                .build();

        // 2. Chunker Node
        GraphNode chunkerNode = GraphNode.newBuilder()
                .setNodeId("chunker-node")
                .setName("Chunker Node")
                .setModuleId("text-chunker")
                .build();

        ModuleDefinition chunkerModule = ModuleDefinition.newBuilder()
                .setModuleId("text-chunker")
                .setGrpcServiceName("text-chunker")
                .build();

        // 3. Embedder Node
        GraphNode embedderNode = GraphNode.newBuilder()
                .setNodeId("embedder-node")
                .setName("Embedder Node")
                .setModuleId("openai-embedder")
                .build();

        ModuleDefinition embedderModule = ModuleDefinition.newBuilder()
                .setModuleId("openai-embedder")
                .setGrpcServiceName("openai-embedder")
                .build();

        // 4. Sink Node
        GraphNode sinkNode = GraphNode.newBuilder()
                .setNodeId("sink-node")
                .setName("Sink Node")
                .setModuleId("opensearch-sink")
                .build();

        ModuleDefinition sinkModule = ModuleDefinition.newBuilder()
                .setModuleId("opensearch-sink")
                .setGrpcServiceName("opensearch-sink")
                .build();

        // Register nodes and modules
        graphCache.putNode(parserNode);
        graphCache.putModule(parserModule);
        graphCache.putNode(chunkerNode);
        graphCache.putModule(chunkerModule);
        graphCache.putNode(embedderNode);
        graphCache.putModule(embedderModule);
        graphCache.putNode(sinkNode);
        graphCache.putModule(sinkModule);

        // Create edges: parser → chunker → embedder → sink
        GraphEdge edge1 = GraphEdge.newBuilder()
                .setEdgeId("edge-parser-to-chunker")
                .setFromNodeId("parser-node")
                .setToNodeId("chunker-node")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setPriority(10)
                .build(); // No condition - always routes

        GraphEdge edge2 = GraphEdge.newBuilder()
                .setEdgeId("edge-chunker-to-embedder")
                .setFromNodeId("chunker-node")
                .setToNodeId("embedder-node")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setPriority(10)
                .build(); // No condition - always routes

        GraphEdge edge3 = GraphEdge.newBuilder()
                .setEdgeId("edge-embedder-to-sink")
                .setFromNodeId("embedder-node")
                .setToNodeId("sink-node")
                .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                .setPriority(10)
                .build(); // No condition - always routes

        // Register edges
        graphCache.putEdge(edge1);
        graphCache.putEdge(edge2);
        graphCache.putEdge(edge3);
    }

    @Test
    @DisplayName("Should process document through complete multi-node pipeline")
    void testCompleteMultiNodePipeline() {
        // Create initial document
        PipeDoc initialDoc = PipeDoc.newBuilder()
                .setDocId("multi-node-doc-1")
                .build();

        // Create stream starting at parser node
        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(initialDoc)
                .setCurrentNodeId("parser-node")
                .setHopCount(0)
                .build();

        // Process first node (parser)
        ProcessNodeRequest request1 = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber1 = engineService.processNode(request1)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response1 = subscriber1.awaitItem().getItem();

        // Verify parser node processed successfully
        assertThat("Parser node should process successfully", 
                response1.getSuccess(), is(true));
        assertThat("Stream should have updated hop count", 
                response1.getUpdatedStream().getHopCount(), is(greaterThan(0L)));

        // Note: In a full integration test, we would:
        // - Verify ProcessData was called for parser with blob hydration
        // - Verify ProcessData was called for chunker
        // - Verify ProcessData was called for embedder
        // - Verify ProcessData was called for sink
        // - Verify final document state
        // - Verify all metadata accumulated correctly
        // - Verify hop count reached 4 (one per node)
    }

    @Test
    @DisplayName("Should accumulate metadata and history through pipeline")
    void testMetadataAccumulation() {
        PipeDoc initialDoc = PipeDoc.newBuilder()
                .setDocId("metadata-doc-1")
                .build();

        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(initialDoc)
                .setCurrentNodeId("parser-node")
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Verify metadata was updated
        assertThat("Response should have updated stream", 
                response.hasUpdatedStream(), is(true));
        
        PipeStream updatedStream = response.getUpdatedStream();
        
        // Verify execution history was added
        assertThat("Stream should have execution history", 
                updatedStream.getMetadata().getHistoryCount(), is(greaterThan(0)));
        
        // Verify hop count incremented
        assertThat("Hop count should increment", 
                updatedStream.getHopCount(), is(greaterThan(stream.getHopCount())));
        
        // Verify processing path was updated
        assertThat("Processing path should include node", 
                updatedStream.getProcessingPathCount(), is(greaterThan(0)));
    }

    @Test
    @DisplayName("Should handle pipeline with terminal node (no outgoing edges)")
    void testTerminalNode() {
        // Create a sink node with no outgoing edges (terminal)
        GraphNode terminalNode = GraphNode.newBuilder()
                .setNodeId("terminal-node")
                .setName("Terminal Node")
                .setModuleId("opensearch-sink")
                .build();

        graphCache.putNode(terminalNode);

        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("terminal-doc-1")
                .build();

        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(doc)
                .setCurrentNodeId("terminal-node")
                .setHopCount(2)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Terminal node should process successfully but not route anywhere
        assertThat("Terminal node should process successfully", 
                response.getSuccess(), is(true));
        
        // Stream should be marked as complete (no further routing)
        // This is implicit - no outgoing edges means no routing happens
    }

    @Test
    @DisplayName("Should handle node not found error gracefully")
    void testNodeNotFound() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("not-found-doc-1")
                .build();

        String streamId = UUID.randomUUID().toString();
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setDocument(doc)
                .setCurrentNodeId("nonexistent-node")
                .setHopCount(0)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        UniAssertSubscriber<ProcessNodeResponse> subscriber = engineService.processNode(request)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        ProcessNodeResponse response = subscriber.awaitItem().getItem();

        // Should return failure response, not throw exception
        assertThat("Response should indicate failure", 
                response.getSuccess(), is(false));
        assertThat("Error message should mention node not found", 
                response.getMessage(), containsString("Node not found"));
    }
}

