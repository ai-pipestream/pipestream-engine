package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.config.v1.TransportType;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class GraphCacheTest {

    @Inject
    GraphCache graphCache;

    @BeforeEach
    void setup() {
        graphCache.clear();
    }

    @Test
    void testNodeStorage() {
        GraphNode node = GraphNode.newBuilder()
                .setNodeId("cluster.node-1")
                .setName("Test Node")
                .setModuleId("parser-module")
                .build();

        graphCache.putNode(node);

        var retrievedOpt = graphCache.getNode("cluster.node-1")
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
        assertTrue(retrievedOpt.isPresent());
        assertEquals("Test Node", retrievedOpt.get().getName());
        assertEquals("parser-module", retrievedOpt.get().getModuleId());
    }

    @Test
    void testEdgeStorageAndSorting() {
        // High priority edge (priority 10)
        GraphEdge edge1 = GraphEdge.newBuilder()
                .setEdgeId("edge-1")
                .setFromNodeId("node-a")
                .setToNodeId("node-b")
                .setPriority(10)
                .build();

        // Higher priority edge (priority 5)
        GraphEdge edge2 = GraphEdge.newBuilder()
                .setEdgeId("edge-2")
                .setFromNodeId("node-a")
                .setToNodeId("node-c")
                .setPriority(5)
                .build();

        graphCache.putEdge(edge1);
        graphCache.putEdge(edge2);

        List<GraphEdge> edges = graphCache.getOutgoingEdges("node-a")
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
        assertEquals(2, edges.size());
        
        // Should be sorted by priority: edge-2 (5) then edge-1 (10)
        assertEquals("edge-2", edges.get(0).getEdgeId());
        assertEquals("edge-1", edges.get(1).getEdgeId());
    }

    @Test
    void testModuleStorage() {
        ModuleDefinition module = ModuleDefinition.newBuilder()
                .setModuleId("parser-v1")
                .setImplementationName("ai.pipestream.Parser")
                .build();

        graphCache.putModule(module);

        var retrievedOpt = graphCache.getModule("parser-v1")
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
        assertTrue(retrievedOpt.isPresent());
        assertEquals("ai.pipestream.Parser", retrievedOpt.get().getImplementationName());
    }

    @Test
    void testEntryNodeRegistration() {
        graphCache.registerEntryNode("ds-123", "node-start");
        
        var nodeIdOpt = graphCache.getEntryNodeId("ds-123")
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
        assertTrue(nodeIdOpt.isPresent());
        assertEquals("node-start", nodeIdOpt.get());
    }
}
