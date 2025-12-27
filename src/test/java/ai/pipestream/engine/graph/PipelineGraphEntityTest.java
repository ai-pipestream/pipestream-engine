package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphMode;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.config.v1.TransportType;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PipelineGraphEntity serialization/deserialization.
 * <p>
 * Verifies that PipelineGraph protobuf messages can be correctly serialized to JSON
 * and deserialized back to protobuf without data loss.
 */
@QuarkusTest
class PipelineGraphEntityTest {

    @Inject
    PipelineGraphService graphService;

    @BeforeEach
    void setup() {
        // Clean up any test data
        // Note: In a real test, you'd use @Transactional or test containers
    }

    /**
     * Tests serialization of a PipelineGraph to JSON and back to protobuf.
     * Verifies that all fields are preserved during the round-trip.
     */
    @Test
    void testSerializationRoundTrip() {
        // Create a complete PipelineGraph with various fields
        PipelineGraph original = PipelineGraph.newBuilder()
                .setGraphId("test-graph-1")
                .setClusterId("test-cluster")
                .setName("Test Pipeline Graph")
                .setDescription("A test graph for serialization testing")
                .setVersion(1L)
                .setMode(GraphMode.GRAPH_MODE_PRODUCTION)
                .addNodeIds("node-1")
                .addNodeIds("node-2")
                .addNodeIds("node-3")
                .addEdges(GraphEdge.newBuilder()
                        .setEdgeId("edge-1")
                        .setFromNodeId("node-1")
                        .setToNodeId("node-2")
                        .setPriority(1)
                        .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                        .setCondition("document.language == 'en'")
                        .build())
                .addEdges(GraphEdge.newBuilder()
                        .setEdgeId("edge-2")
                        .setFromNodeId("node-2")
                        .setToNodeId("node-3")
                        .setPriority(2)
                        .setTransportType(TransportType.TRANSPORT_TYPE_MESSAGING)
                        .setKafkaTopic("custom-topic")
                        .build())
                .setCreatedAt(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond())
                        .setNanos(Instant.now().getNano())
                        .build())
                .build();

        // Serialize to entity
        PipelineGraphEntity entity = PipelineGraphEntity.fromProto(
                original, 
                "test-account", 
                "test-user", 
                true
        );

        // Verify entity fields
        assertNotNull(entity.graphData);
        assertFalse(entity.graphData.isEmpty());
        assertEquals("test-graph-1", entity.graphId);
        assertEquals("test-cluster", entity.clusterId);
        assertEquals("test-account", entity.accountId);
        assertEquals(1L, entity.version);
        assertTrue(entity.isActive);
        assertNotNull(entity.createdAt);
        assertEquals("test-user", entity.createdBy);

        // Deserialize back to protobuf
        PipelineGraph deserialized = entity.toProto();

        // Verify all fields match
        assertEquals(original.getGraphId(), deserialized.getGraphId());
        assertEquals(original.getClusterId(), deserialized.getClusterId());
        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getDescription(), deserialized.getDescription());
        assertEquals(original.getVersion(), deserialized.getVersion());
        assertEquals(original.getMode(), deserialized.getMode());
        assertEquals(original.getNodeIdsCount(), deserialized.getNodeIdsCount());
        assertEquals(original.getNodeIds(0), deserialized.getNodeIds(0));
        assertEquals(original.getNodeIds(1), deserialized.getNodeIds(1));
        assertEquals(original.getNodeIds(2), deserialized.getNodeIds(2));
        assertEquals(original.getEdgesCount(), deserialized.getEdgesCount());
        
        // Verify first edge
        GraphEdge edge1 = deserialized.getEdges(0);
        assertEquals("edge-1", edge1.getEdgeId());
        assertEquals("node-1", edge1.getFromNodeId());
        assertEquals("node-2", edge1.getToNodeId());
        assertEquals(1, edge1.getPriority());
        assertEquals(TransportType.TRANSPORT_TYPE_GRPC, edge1.getTransportType());
        assertEquals("document.language == 'en'", edge1.getCondition());
        
        // Verify second edge
        GraphEdge edge2 = deserialized.getEdges(1);
        assertEquals("edge-2", edge2.getEdgeId());
        assertEquals("node-2", edge2.getFromNodeId());
        assertEquals("node-3", edge2.getToNodeId());
        assertEquals(2, edge2.getPriority());
        assertEquals(TransportType.TRANSPORT_TYPE_MESSAGING, edge2.getTransportType());
        assertEquals("custom-topic", edge2.getKafkaTopic());

        // Verify timestamps
        assertEquals(original.getCreatedAt().getSeconds(), deserialized.getCreatedAt().getSeconds());
    }

    /**
     * Tests serialization with minimal graph (only required fields).
     */
    @Test
    void testMinimalGraphSerialization() {
        PipelineGraph minimal = PipelineGraph.newBuilder()
                .setGraphId("minimal-graph")
                .setClusterId("cluster-1")
                .setVersion(1L)
                .build();

        PipelineGraphEntity entity = PipelineGraphEntity.fromProto(minimal, "account-1", "user-1", false);
        PipelineGraph deserialized = entity.toProto();

        assertEquals(minimal.getGraphId(), deserialized.getGraphId());
        assertEquals(minimal.getClusterId(), deserialized.getClusterId());
        assertEquals(minimal.getVersion(), deserialized.getVersion());
        assertEquals(0, deserialized.getNodeIdsCount());
        assertEquals(0, deserialized.getEdgesCount());
    }

    /**
     * Tests that JSONB storage preserves complex nested structures.
     */
    @Test
    void testComplexGraphSerialization() {
        PipelineGraph complex = PipelineGraph.newBuilder()
                .setGraphId("complex-graph")
                .setClusterId("cluster-1")
                .setName("Complex Pipeline")
                .setDescription("A graph with many nodes and edges")
                .setVersion(42L)
                .setMode(GraphMode.GRAPH_MODE_DESIGN)
                .addNodeIds("parser-node")
                .addNodeIds("chunker-node")
                .addNodeIds("embedder-node")
                .addNodeIds("sink-node")
                .addEdges(GraphEdge.newBuilder()
                        .setEdgeId("e1")
                        .setFromNodeId("parser-node")
                        .setToNodeId("chunker-node")
                        .setPriority(1)
                        .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                        .setCondition("document.size < 1000000")
                        .build())
                .addEdges(GraphEdge.newBuilder()
                        .setEdgeId("e2")
                        .setFromNodeId("chunker-node")
                        .setToNodeId("embedder-node")
                        .setPriority(1)
                        .setTransportType(TransportType.TRANSPORT_TYPE_MESSAGING)
                        .setKafkaTopic("chunks-to-embed")
                        .setIsCrossCluster(true)
                        .setToClusterId("embed-cluster")
                        .build())
                .addEdges(GraphEdge.newBuilder()
                        .setEdgeId("e3")
                        .setFromNodeId("embedder-node")
                        .setToNodeId("sink-node")
                        .setPriority(1)
                        .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                        .setMaxHops(10)
                        .build())
                .build();

        PipelineGraphEntity entity = PipelineGraphEntity.fromProto(complex, "account-1", "system", true);
        PipelineGraph deserialized = entity.toProto();

        // Verify structure
        assertEquals(4, deserialized.getNodeIdsCount());
        assertEquals(3, deserialized.getEdgesCount());
        
        // Verify cross-cluster edge
        GraphEdge crossClusterEdge = deserialized.getEdges(1);
        assertTrue(crossClusterEdge.getIsCrossCluster());
        assertEquals("embed-cluster", crossClusterEdge.getToClusterId());
        assertEquals("chunks-to-embed", crossClusterEdge.getKafkaTopic());
        
        // Verify max hops
        GraphEdge maxHopsEdge = deserialized.getEdges(2);
        assertEquals(10, maxHopsEdge.getMaxHops());
    }

    /**
     * Tests that invalid JSON in graph_data throws appropriate exception.
     */
    @Test
    void testInvalidJsonDeserialization() {
        PipelineGraphEntity entity = new PipelineGraphEntity();
        entity.graphId = "test-graph";
        entity.clusterId = "cluster-1";
        entity.accountId = "account-1";
        entity.version = 1L;
        entity.graphData = "invalid json {";
        entity.isActive = false;
        entity.createdAt = Instant.now();

        assertThrows(RuntimeException.class, entity::toProto);
    }

    /**
     * Tests the fromProto factory method with all parameters.
     */
    @Test
    void testFromProtoWithAllParameters() {
        PipelineGraph graph = PipelineGraph.newBuilder()
                .setGraphId("test-graph")
                .setClusterId("cluster-1")
                .setVersion(5L)
                .build();

        PipelineGraphEntity entity = PipelineGraphEntity.fromProto(
                graph, 
                "account-123", 
                "admin-user", 
                true
        );

        assertEquals("test-graph", entity.graphId);
        assertEquals("cluster-1", entity.clusterId);
        assertEquals("account-123", entity.accountId);
        assertEquals(5L, entity.version);
        assertEquals("admin-user", entity.createdBy);
        assertTrue(entity.isActive);
        assertNotNull(entity.createdAt);
        assertNotNull(entity.graphData);
    }
}

