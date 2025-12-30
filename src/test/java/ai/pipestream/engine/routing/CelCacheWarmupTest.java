package ai.pipestream.engine.routing;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.data.v1.CelConfig;
import ai.pipestream.data.v1.ProcessingMapping;
import ai.pipestream.engine.graph.GraphCache;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests for CEL cache warmup functionality (Issue #1).
 * <p>
 * Verifies that:
 * - GraphCache fires GraphLoadedEvent when a graph is loaded
 * - CelEvaluatorService observes the event and pre-compiles expressions
 * - All expression types are collected (edge conditions, filters, mappings)
 */
@QuarkusTest
class CelCacheWarmupTest {

    @Inject
    GraphCache graphCache;

    @Inject
    CelEvaluatorService celEvaluatorService;

    @BeforeEach
    void setUp() {
        graphCache.clear();
        celEvaluatorService.clearCache();
    }

    @Test
    @DisplayName("Should pre-compile edge conditions on graph load")
    void testEdgeConditionWarmup() {
        // Create graph with edge conditions
        GraphEdge edge1 = GraphEdge.newBuilder()
                .setEdgeId("edge-1")
                .setFromNodeId("node-1")
                .setToNodeId("node-2")
                .setCondition("document.doc_id != ''")
                .build();

        GraphEdge edge2 = GraphEdge.newBuilder()
                .setEdgeId("edge-2")
                .setFromNodeId("node-1")
                .setToNodeId("node-3")
                .setCondition("stream.hop_count < 10")
                .build();

        GraphNode node1 = GraphNode.newBuilder()
                .setNodeId("node-1")
                .setName("Node 1")
                .setModuleId("module-1")
                .build();

        GraphNode node2 = GraphNode.newBuilder()
                .setNodeId("node-2")
                .setName("Node 2")
                .setModuleId("module-2")
                .build();

        GraphNode node3 = GraphNode.newBuilder()
                .setNodeId("node-3")
                .setName("Node 3")
                .setModuleId("module-3")
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(node1)
                .addNodes(node2)
                .addNodes(node3)
                .addEdges(edge1)
                .addEdges(edge2)
                .build();

        assertThat("Cache should be empty before load", celEvaluatorService.getCacheSize(), is(0));

        // Load graph - this should trigger async warmup
        graphCache.loadGraph(graph);

        // Wait for async warmup to complete (uses @ObservesAsync)
        await().atMost(5, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                    assertThat("Cache should contain compiled expressions",
                            celEvaluatorService.getCacheSize(), greaterThanOrEqualTo(2)));
    }

    @Test
    @DisplayName("Should pre-compile node filter conditions on graph load")
    void testFilterConditionWarmup() {
        GraphNode node = GraphNode.newBuilder()
                .setNodeId("filter-node")
                .setName("Filter Node")
                .setModuleId("module-1")
                .addFilterConditions("document.search_metadata.title != ''")
                .addFilterConditions("document.doc_id.startsWith('doc-')")
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(node)
                .build();

        assertThat("Cache should be empty before load", celEvaluatorService.getCacheSize(), is(0));

        graphCache.loadGraph(graph);

        await().atMost(5, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                    assertThat("Cache should contain filter expressions",
                            celEvaluatorService.getCacheSize(), greaterThanOrEqualTo(2)));
    }

    @Test
    @DisplayName("Should pre-compile CEL mappings on graph load")
    void testMappingCelWarmup() {
        ProcessingMapping preMapping = ProcessingMapping.newBuilder()
                .setMappingId("pre-map-1")
                .setCelConfig(CelConfig.newBuilder()
                        .setExpression("document.search_metadata.relevance_score * 100")
                        .build())
                .build();

        ProcessingMapping postMapping = ProcessingMapping.newBuilder()
                .setMappingId("post-map-1")
                .setCelConfig(CelConfig.newBuilder()
                        .setExpression("string(stream.hop_count)")
                        .build())
                .build();

        GraphNode node = GraphNode.newBuilder()
                .setNodeId("mapping-node")
                .setName("Mapping Node")
                .setModuleId("module-1")
                .addPreMappings(preMapping)
                .addPostMappings(postMapping)
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(node)
                .build();

        assertThat("Cache should be empty before load", celEvaluatorService.getCacheSize(), is(0));

        graphCache.loadGraph(graph);

        await().atMost(5, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                    assertThat("Cache should contain mapping expressions",
                            celEvaluatorService.getCacheSize(), greaterThanOrEqualTo(2)));
    }

    @Test
    @DisplayName("Should deduplicate identical expressions")
    void testExpressionDeduplication() {
        String sharedCondition = "document.doc_id != ''";

        GraphEdge edge1 = GraphEdge.newBuilder()
                .setEdgeId("edge-1")
                .setFromNodeId("node-1")
                .setToNodeId("node-2")
                .setCondition(sharedCondition)
                .build();

        GraphEdge edge2 = GraphEdge.newBuilder()
                .setEdgeId("edge-2")
                .setFromNodeId("node-2")
                .setToNodeId("node-3")
                .setCondition(sharedCondition) // Same condition
                .build();

        GraphNode node1 = GraphNode.newBuilder()
                .setNodeId("node-1")
                .setName("Node 1")
                .setModuleId("module-1")
                .addFilterConditions(sharedCondition) // Same condition again
                .build();

        GraphNode node2 = GraphNode.newBuilder()
                .setNodeId("node-2")
                .setName("Node 2")
                .setModuleId("module-2")
                .build();

        GraphNode node3 = GraphNode.newBuilder()
                .setNodeId("node-3")
                .setName("Node 3")
                .setModuleId("module-3")
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(node1)
                .addNodes(node2)
                .addNodes(node3)
                .addEdges(edge1)
                .addEdges(edge2)
                .build();

        graphCache.loadGraph(graph);

        await().atMost(5, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                    // Should only compile the expression once despite appearing 3 times
                    assertThat("Duplicate expressions should be deduplicated",
                            celEvaluatorService.getCacheSize(), is(1)));
    }

    @Test
    @DisplayName("Should handle graph with no CEL expressions")
    void testGraphWithNoExpressions() {
        GraphNode node = GraphNode.newBuilder()
                .setNodeId("simple-node")
                .setName("Simple Node")
                .setModuleId("module-1")
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(node)
                .build();

        graphCache.loadGraph(graph);

        // Should complete without error, cache remains empty
        await().atMost(2, TimeUnit.SECONDS)
                .pollDelay(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                    assertThat("Cache should remain empty for graph with no expressions",
                            celEvaluatorService.getCacheSize(), is(0)));
    }

    @Test
    @DisplayName("Should handle graph reload with new expressions")
    void testGraphReloadWarmup() {
        // First graph
        GraphNode node1 = GraphNode.newBuilder()
                .setNodeId("node-1")
                .setName("Node 1")
                .setModuleId("module-1")
                .addFilterConditions("document.doc_id != ''")
                .build();

        PipelineGraph graph1 = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph V1")
                .addNodes(node1)
                .build();

        graphCache.loadGraph(graph1);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() ->
                    assertThat(celEvaluatorService.getCacheSize(), is(1)));

        // Reload with different expressions
        GraphNode node2 = GraphNode.newBuilder()
                .setNodeId("node-2")
                .setName("Node 2")
                .setModuleId("module-2")
                .addFilterConditions("stream.hop_count < 5")
                .addFilterConditions("document.search_metadata.title.size() > 0")
                .build();

        PipelineGraph graph2 = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(2)
                .setName("Test Graph V2")
                .addNodes(node2)
                .build();

        graphCache.loadGraph(graph2);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() ->
                    // Original expression + 2 new ones = 3 total
                    assertThat("Cache should contain all expressions from both loads",
                            celEvaluatorService.getCacheSize(), is(3)));
    }
}
