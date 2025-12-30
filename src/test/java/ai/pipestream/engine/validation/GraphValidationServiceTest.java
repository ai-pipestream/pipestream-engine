package ai.pipestream.engine.validation;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.engine.graph.GraphCache;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Unit tests for {@link GraphValidationService}.
 * <p>
 * Tests validate that the service correctly:
 * - Validates node existence in the active graph
 * - Validates edge targets exist
 * - Validates module registration
 * - Returns appropriate error messages for validation failures
 */
@QuarkusTest
class GraphValidationServiceTest {

    @Inject
    GraphValidationService validationService;

    @Inject
    GraphCache graphCache;

    @BeforeEach
    void setUp() {
        graphCache.clear();
    }

    @Test
    @DisplayName("Should validate node exists in active graph")
    void testValidateNodeExists_Success() {
        // Setup: Create a graph with a node
        GraphNode node = GraphNode.newBuilder()
                .setNodeId("test-node-1")
                .setName("Test Node 1")
                .setModuleId("test-module")
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(node)
                .build();

        graphCache.loadGraph(graph);

        // Validate node exists
        UniAssertSubscriber<GraphNode> subscriber = validationService.validateNodeExists("test-node-1", "account-1")
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        GraphNode validatedNode = subscriber.awaitItem().getItem();
        assertThat("Validated node should match", validatedNode.getNodeId(), is("test-node-1"));
        assertThat("Validated node should match", validatedNode.getName(), is("Test Node 1"));
    }

    @Test
    @DisplayName("Should fail validation when node does not exist")
    void testValidateNodeExists_NodeNotFound() {
        // Setup: Create a graph with a node
        GraphNode node = GraphNode.newBuilder()
                .setNodeId("test-node-1")
                .setName("Test Node 1")
                .setModuleId("test-module")
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(node)
                .build();

        graphCache.loadGraph(graph);

        // Try to validate non-existent node
        UniAssertSubscriber<GraphNode> subscriber = validationService.validateNodeExists("non-existent-node", "account-1")
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure();
        Throwable failure = subscriber.getFailure();
        assertThat("Failure should be NodeNotFoundException", failure, instanceOf(NodeNotFoundException.class));
        NodeNotFoundException nfe = (NodeNotFoundException) failure;
        assertThat("Error should include node ID", nfe.getNodeId(), is("non-existent-node"));
        assertThat("Error should list available nodes", nfe.getAvailableNodeIds(), hasItem("test-node-1"));
    }

    @Test
    @DisplayName("Should fail validation when no graph is loaded")
    void testValidateNodeExists_NoGraphLoaded() {
        // No graph loaded

        // Try to validate node
        UniAssertSubscriber<GraphNode> subscriber = validationService.validateNodeExists("test-node-1", "account-1")
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure();
        Throwable failure = subscriber.getFailure();
        assertThat("Failure should be NodeNotFoundException", failure, instanceOf(NodeNotFoundException.class));
        NodeNotFoundException nfe = (NodeNotFoundException) failure;
        assertThat("Error should include node ID", nfe.getNodeId(), is("test-node-1"));
        assertThat("Available nodes should be empty", nfe.getAvailableNodeIds(), empty());
    }

    @Test
    @DisplayName("Should validate edge targets exist")
    void testValidateEdgeTargets_Success() {
        // Setup: Create a graph with nodes and valid edges
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

        GraphEdge edge = GraphEdge.newBuilder()
                .setEdgeId("edge-1")
                .setFromNodeId("node-1")
                .setToNodeId("node-2")
                .setPriority(1)
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(node1)
                .addNodes(node2)
                .addEdges(edge)
                .build();

        graphCache.loadGraph(graph);

        // Validate edge targets
        UniAssertSubscriber<Void> subscriber = validationService.validateEdgeTargets(node1, graph)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitItem();
        // Should complete successfully (no failure)
    }

    @Test
    @DisplayName("Should fail validation when edge targets non-existent node")
    void testValidateEdgeTargets_InvalidTarget() {
        // Setup: Create a graph with node and edge pointing to non-existent node
        GraphNode node1 = GraphNode.newBuilder()
                .setNodeId("node-1")
                .setName("Node 1")
                .setModuleId("module-1")
                .build();

        GraphEdge edge = GraphEdge.newBuilder()
                .setEdgeId("edge-1")
                .setFromNodeId("node-1")
                .setToNodeId("non-existent-node")
                .setPriority(1)
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(node1)
                .addEdges(edge)
                .build();

        graphCache.loadGraph(graph);

        // Validate edge targets - should fail
        UniAssertSubscriber<Void> subscriber = validationService.validateEdgeTargets(node1, graph)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure();
        Throwable failure = subscriber.getFailure();
        assertThat("Failure should be StatusRuntimeException", failure, instanceOf(io.grpc.StatusRuntimeException.class));
        assertThat("Error message should mention invalid target", failure.getMessage(), containsString("non-existent-node"));
    }

    @Test
    @DisplayName("Should validate module is registered")
    void testValidateModuleRegistered_Success() {
        // Setup: Register a module
        ModuleDefinition module = ModuleDefinition.newBuilder()
                .setModuleId("test-module")
                .setGrpcServiceName("test-module-service")
                .build();

        graphCache.putModule(module);

        // Validate module is registered
        UniAssertSubscriber<Void> subscriber = validationService.validateModuleRegistered("test-module")
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitItem();
        // Should complete successfully (no failure)
    }

    @Test
    @DisplayName("Should fail validation when module is not registered")
    void testValidateModuleRegistered_ModuleNotFound() {
        // No module registered

        // Try to validate non-existent module
        UniAssertSubscriber<Void> subscriber = validationService.validateModuleRegistered("non-existent-module")
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure();
        Throwable failure = subscriber.getFailure();
        assertThat("Failure should be StatusRuntimeException", failure, instanceOf(io.grpc.StatusRuntimeException.class));
        io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) failure;
        assertThat("Status should be UNAVAILABLE", sre.getStatus().getCode(), is(io.grpc.Status.Code.UNAVAILABLE));
        assertThat("Error message should mention module", failure.getMessage(), containsString("non-existent-module"));
    }

    @Test
    @DisplayName("Should handle terminal node with no outgoing edges")
    void testValidateEdgeTargets_TerminalNode() {
        // Setup: Create a terminal node (no outgoing edges)
        GraphNode terminalNode = GraphNode.newBuilder()
                .setNodeId("terminal-node")
                .setName("Terminal Node")
                .setModuleId("module-1")
                .build();

        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setName("Test Graph")
                .addNodes(terminalNode)
                .build();

        graphCache.loadGraph(graph);

        // Validate edge targets for terminal node
        UniAssertSubscriber<Void> subscriber = validationService.validateEdgeTargets(terminalNode, graph)
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitItem();
        // Should complete successfully (terminal nodes have no edges to validate)
    }
}

