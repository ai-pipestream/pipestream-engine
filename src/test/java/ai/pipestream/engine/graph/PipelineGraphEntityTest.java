package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphMode;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.NodeType;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.config.v1.TransportType;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests for PipelineGraphEntity serialization/deserialization.
 * <p>
 * Verifies that PipelineGraph protobuf messages can be correctly serialized to JSON
 * and deserialized back to protobuf without data loss.
 */
@QuarkusTest
class PipelineGraphEntityTest {

    /**
     * Tests serialization of a PipelineGraph to JSON and back to protobuf.
     * Verifies that all fields are preserved during the round-trip.
     * <p>
     * Note: This test doesn't require database access - it only tests serialization/deserialization logic.
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
                .addNodes(GraphNode.newBuilder()
                        .setNodeId("node-1")
                        .setClusterId("test-cluster")
                        .setName("Node 1")
                        .setNodeType(NodeType.NODE_TYPE_PROCESSOR)
                        .build())
                .addNodes(GraphNode.newBuilder()
                        .setNodeId("node-2")
                        .setClusterId("test-cluster")
                        .setName("Node 2")
                        .setNodeType(NodeType.NODE_TYPE_PROCESSOR)
                        .build())
                .addNodes(GraphNode.newBuilder()
                        .setNodeId("node-3")
                        .setClusterId("test-cluster")
                        .setName("Node 3")
                        .setNodeType(NodeType.NODE_TYPE_PROCESSOR)
                        .build())
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
                "test-user", 
                true
        );

        // Verify entity fields
        assertThat(entity.graphData, is(notNullValue()));
        assertThat(entity.graphData, is(not(emptyString())));
        assertThat(entity.graphId, is("test-graph-1"));
        assertThat(entity.clusterId, is("test-cluster"));
        assertThat(entity.version, is(1L));
        assertThat(entity.isActive, is(true));
        assertThat(entity.createdAt, is(notNullValue()));
        assertThat(entity.createdBy, is("test-user"));

        // Deserialize back to protobuf
        PipelineGraph deserialized = entity.toProto();

        // Verify all fields match
        assertThat(deserialized.getGraphId(), is(original.getGraphId()));
        assertThat(deserialized.getClusterId(), is(original.getClusterId()));
        assertThat(deserialized.getName(), is(original.getName()));
        assertThat(deserialized.getDescription(), is(original.getDescription()));
        assertThat(deserialized.getVersion(), is(original.getVersion()));
        assertThat(deserialized.getMode(), is(original.getMode()));
        assertThat(deserialized.getNodesCount(), is(original.getNodesCount()));
        assertThat(deserialized.getNodes(0).getNodeId(), is(original.getNodes(0).getNodeId()));
        assertThat(deserialized.getNodes(1).getNodeId(), is(original.getNodes(1).getNodeId()));
        assertThat(deserialized.getNodes(2).getNodeId(), is(original.getNodes(2).getNodeId()));
        assertThat(deserialized.getEdgesCount(), is(original.getEdgesCount()));
        
        // Verify first edge
        GraphEdge edge1 = deserialized.getEdges(0);
        assertThat(edge1.getEdgeId(), is("edge-1"));
        assertThat(edge1.getFromNodeId(), is("node-1"));
        assertThat(edge1.getToNodeId(), is("node-2"));
        assertThat(edge1.getPriority(), is(1));
        assertThat(edge1.getTransportType(), is(TransportType.TRANSPORT_TYPE_GRPC));
        assertThat(edge1.getCondition(), is("document.language == 'en'"));
        
        // Verify second edge
        GraphEdge edge2 = deserialized.getEdges(1);
        assertThat(edge2.getEdgeId(), is("edge-2"));
        assertThat(edge2.getFromNodeId(), is("node-2"));
        assertThat(edge2.getToNodeId(), is("node-3"));
        assertThat(edge2.getPriority(), is(2));
        assertThat(edge2.getTransportType(), is(TransportType.TRANSPORT_TYPE_MESSAGING));
        assertThat(edge2.getKafkaTopic(), is("custom-topic"));

        // Verify timestamps
        assertThat(deserialized.getCreatedAt().getSeconds(), is(original.getCreatedAt().getSeconds()));
    }

    /**
     * Tests serialization with minimal graph (only required fields).
     * <p>
     * Note: This test doesn't require database access - it only tests serialization/deserialization logic.
     */
    @Test
    void testMinimalGraphSerialization() {
        PipelineGraph minimal = PipelineGraph.newBuilder()
                .setGraphId("minimal-graph")
                .setClusterId("cluster-1")
                .setVersion(1L)
                .build();

        PipelineGraphEntity entity = PipelineGraphEntity.fromProto(minimal, "user-1", false);
        PipelineGraph deserialized = entity.toProto();

        assertThat(deserialized.getGraphId(), is(minimal.getGraphId()));
        assertThat(deserialized.getClusterId(), is(minimal.getClusterId()));
        assertThat(deserialized.getVersion(), is(minimal.getVersion()));
        assertThat(deserialized.getNodesCount(), is(0));
        assertThat(deserialized.getEdgesCount(), is(0));
    }

    /**
     * Tests that JSONB storage preserves complex nested structures.
     * <p>
     * Note: This test doesn't require database access - it only tests serialization/deserialization logic.
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
                .addNodes(GraphNode.newBuilder()
                        .setNodeId("parser-node")
                        .setClusterId("cluster-1")
                        .setName("Parser Node")
                        .setNodeType(NodeType.NODE_TYPE_PROCESSOR)
                        .build())
                .addNodes(GraphNode.newBuilder()
                        .setNodeId("chunker-node")
                        .setClusterId("cluster-1")
                        .setName("Chunker Node")
                        .setNodeType(NodeType.NODE_TYPE_PROCESSOR)
                        .build())
                .addNodes(GraphNode.newBuilder()
                        .setNodeId("embedder-node")
                        .setClusterId("cluster-1")
                        .setName("Embedder Node")
                        .setNodeType(NodeType.NODE_TYPE_PROCESSOR)
                        .build())
                .addNodes(GraphNode.newBuilder()
                        .setNodeId("sink-node")
                        .setClusterId("cluster-1")
                        .setName("Sink Node")
                        .setNodeType(NodeType.NODE_TYPE_SINK)
                        .build())
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

        PipelineGraphEntity entity = PipelineGraphEntity.fromProto(complex, "system", true);
        PipelineGraph deserialized = entity.toProto();

        // Verify structure
        assertThat(deserialized.getNodesCount(), is(4));
        assertThat(deserialized.getEdgesCount(), is(3));
        
        // Verify cross-cluster edge
        GraphEdge crossClusterEdge = deserialized.getEdges(1);
        assertThat(crossClusterEdge.getIsCrossCluster(), is(true));
        assertThat(crossClusterEdge.getToClusterId(), is("embed-cluster"));
        assertThat(crossClusterEdge.getKafkaTopic(), is("chunks-to-embed"));
        
        // Verify max hops
        GraphEdge maxHopsEdge = deserialized.getEdges(2);
        assertThat(maxHopsEdge.getMaxHops(), is(10));
    }

    /**
     * Tests that invalid JSON in graph_data throws appropriate exception.
     * <p>
     * Note: This test doesn't require database access - it only tests error handling in deserialization.
     */
    @Test
    void testInvalidJsonDeserialization() {
        PipelineGraphEntity entity = new PipelineGraphEntity();
        entity.graphId = "test-graph";
        entity.clusterId = "cluster-1";
        entity.version = 1L;
        entity.graphData = "invalid json {";
        entity.isActive = false;
        entity.createdAt = Instant.now();

        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, entity::toProto);
    }

    /**
     * Tests the fromProto factory method with all parameters.
     * <p>
     * Note: This test doesn't require database access - it only tests entity creation logic.
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
                "admin-user", 
                true
        );

        assertThat(entity.graphId, is("test-graph"));
        assertThat(entity.clusterId, is("cluster-1"));
        assertThat(entity.version, is(5L));
        assertThat(entity.createdBy, is("admin-user"));
        assertThat(entity.isActive, is(true));
        assertThat(entity.createdAt, is(notNullValue()));
        assertThat(entity.graphData, is(notNullValue()));
    }
}

