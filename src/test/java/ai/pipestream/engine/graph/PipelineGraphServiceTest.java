package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphMode;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.config.v1.TransportType;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.QuarkusTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for PipelineGraphService CRUD operations.
 * <p>
 * Tests require a PostgreSQL database (provided by Quarkus DevServices or Testcontainers).
 * All operations are tested using reactive Mutiny Uni.
 */
@QuarkusTest
class PipelineGraphServiceTest {

    @Inject
    PipelineGraphService graphService;

    private static final String TEST_GRAPH_ID = "test-graph-crud";
    private static final String TEST_CLUSTER_ID = "test-cluster";
    private static final String TEST_ACCOUNT_ID = "test-account";
    private static final String TEST_CREATED_BY = "test-user";

    @BeforeEach
    void setup() {
        // Clean up test data before each test
        graphService.deleteAll(TEST_GRAPH_ID).await().indefinitely();
    }

    /**
     * Tests creating a new graph version.
     */
    @Test
    void testCreate() {
        PipelineGraph graph = createTestGraph(1L);
        
        PipelineGraphEntity entity = graphService.create(graph, TEST_ACCOUNT_ID, TEST_CREATED_BY)
                .await().indefinitely();

        assertNotNull(entity);
        assertNotNull(entity.id);
        assertEquals(TEST_GRAPH_ID, entity.graphId);
        assertEquals(TEST_CLUSTER_ID, entity.clusterId);
        assertEquals(TEST_ACCOUNT_ID, entity.accountId);
        assertEquals(1L, entity.version);
        assertFalse(entity.isActive); // Default to inactive
        assertNotNull(entity.createdAt);
        assertEquals(TEST_CREATED_BY, entity.createdBy);
        assertNotNull(entity.graphData);
    }

    /**
     * Tests creating and activating a graph version.
     */
    @Test
    void testCreateAndActivate() {
        PipelineGraph graph1 = createTestGraph(1L);
        PipelineGraph graph2 = createTestGraph(2L);

        // Create first version and activate
        PipelineGraphEntity entity1 = graphService.createAndActivate(graph1, TEST_ACCOUNT_ID, TEST_CREATED_BY)
                .await().indefinitely();
        assertTrue(entity1.isActive);

        // Create second version and activate - should deactivate first
        PipelineGraphEntity entity2 = graphService.createAndActivate(graph2, TEST_ACCOUNT_ID, TEST_CREATED_BY)
                .await().indefinitely();
        assertTrue(entity2.isActive);

        // Verify first version is now inactive
        PipelineGraphEntity reloaded1 = graphService.findById(entity1.id).await().indefinitely();
        assertFalse(reloaded1.isActive);
    }

    /**
     * Tests finding a graph by ID.
     */
    @Test
    void testFindById() {
        PipelineGraph graph = createTestGraph(1L);
        PipelineGraphEntity created = graphService.create(graph, TEST_ACCOUNT_ID, TEST_CREATED_BY)
                .await().indefinitely();

        PipelineGraphEntity found = graphService.findById(created.id).await().indefinitely();

        assertNotNull(found);
        assertEquals(created.id, found.id);
        assertEquals(TEST_GRAPH_ID, found.graphId);
    }

    /**
     * Tests finding a graph by graph_id and version.
     */
    @Test
    void testFindByGraphIdAndVersion() {
        PipelineGraph graph1 = createTestGraph(1L);
        PipelineGraph graph2 = createTestGraph(2L);

        graphService.create(graph1, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        graphService.create(graph2, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();

        PipelineGraphEntity found = graphService.findByGraphIdAndVersion(TEST_GRAPH_ID, 2L)
                .await().indefinitely();

        assertNotNull(found);
        assertEquals(2L, found.version);
    }

    /**
     * Tests finding the active graph for a graph_id and cluster.
     */
    @Test
    void testFindActive() {
        PipelineGraph graph1 = createTestGraph(1L);
        PipelineGraph graph2 = createTestGraph(2L);

        graphService.create(graph1, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        graphService.createAndActivate(graph2, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();

        PipelineGraphEntity active = graphService.findActive(TEST_GRAPH_ID, TEST_CLUSTER_ID)
                .await().indefinitely();

        assertNotNull(active);
        assertEquals(2L, active.version);
        assertTrue(active.isActive);
    }

    /**
     * Tests finding all active graphs for a cluster.
     */
    @Test
    void testFindActiveByCluster() {
        PipelineGraph graph1 = createTestGraph("graph-1", 1L);
        PipelineGraph graph2 = createTestGraph("graph-2", 1L);
        PipelineGraph graph3 = createTestGraph("graph-3", 1L);

        graphService.createAndActivate(graph1, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        graphService.createAndActivate(graph2, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        graphService.create(graph3, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely(); // Inactive

        List<PipelineGraphEntity> active = graphService.findActiveByCluster(TEST_CLUSTER_ID)
                .await().indefinitely();

        assertEquals(2, active.size());
        assertTrue(active.stream().allMatch(e -> e.isActive));
    }

    /**
     * Tests finding all versions of a graph.
     */
    @Test
    void testFindAllVersions() {
        PipelineGraph graph1 = createTestGraph(1L);
        PipelineGraph graph2 = createTestGraph(2L);
        PipelineGraph graph3 = createTestGraph(3L);

        graphService.create(graph1, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        graphService.create(graph2, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        graphService.create(graph3, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();

        List<PipelineGraphEntity> versions = graphService.findAllVersions(TEST_GRAPH_ID)
                .await().indefinitely();

        assertEquals(3, versions.size());
        // Should be ordered by version DESC
        assertEquals(3L, versions.get(0).version);
        assertEquals(2L, versions.get(1).version);
        assertEquals(1L, versions.get(2).version);
    }

    /**
     * Tests getting the maximum version number.
     */
    @Test
    void testGetMaxVersion() {
        assertEquals(0L, graphService.getMaxVersion(TEST_GRAPH_ID).await().indefinitely());

        graphService.create(createTestGraph(1L), TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        assertEquals(1L, graphService.getMaxVersion(TEST_GRAPH_ID).await().indefinitely());

        graphService.create(createTestGraph(5L), TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        assertEquals(5L, graphService.getMaxVersion(TEST_GRAPH_ID).await().indefinitely());
    }

    /**
     * Tests activating a specific version.
     */
    @Test
    void testActivate() {
        PipelineGraph graph1 = createTestGraph(1L);
        PipelineGraph graph2 = createTestGraph(2L);

        PipelineGraphEntity entity1 = graphService.createAndActivate(graph1, TEST_ACCOUNT_ID, TEST_CREATED_BY)
                .await().indefinitely();
        PipelineGraphEntity entity2 = graphService.create(graph2, TEST_ACCOUNT_ID, TEST_CREATED_BY)
                .await().indefinitely();

        assertTrue(entity1.isActive);
        assertFalse(entity2.isActive);

        // Activate version 2
        graphService.activate(TEST_GRAPH_ID, 2L).await().indefinitely();

        // Verify both are updated
        PipelineGraphEntity reloaded1 = graphService.findById(entity1.id).await().indefinitely();
        PipelineGraphEntity reloaded2 = graphService.findById(entity2.id).await().indefinitely();
        assertFalse(reloaded1.isActive);
        assertTrue(reloaded2.isActive);
    }

    /**
     * Tests deactivating all versions.
     */
    @Test
    void testDeactivateAll() {
        PipelineGraph graph1 = createTestGraph(1L);
        PipelineGraph graph2 = createTestGraph(2L);

        graphService.createAndActivate(graph1, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        graphService.create(graph2, TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();

        graphService.deactivateAll(TEST_GRAPH_ID, TEST_CLUSTER_ID).await().indefinitely();

        PipelineGraphEntity active = graphService.findActive(TEST_GRAPH_ID, TEST_CLUSTER_ID)
                .await().indefinitely();
        assertNull(active);
    }

    /**
     * Tests deleting a specific version.
     */
    @Test
    void testDelete() {
        PipelineGraph graph = createTestGraph(1L);
        PipelineGraphEntity created = graphService.create(graph, TEST_ACCOUNT_ID, TEST_CREATED_BY)
                .await().indefinitely();

        boolean deleted = graphService.delete(TEST_GRAPH_ID, 1L).await().indefinitely();
        assertTrue(deleted);

        PipelineGraphEntity found = graphService.findById(created.id).await().indefinitely();
        assertNull(found);
    }

    /**
     * Tests deleting all versions of a graph.
     */
    @Test
    void testDeleteAll() {
        graphService.create(createTestGraph(1L), TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        graphService.create(createTestGraph(2L), TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();
        graphService.create(createTestGraph(3L), TEST_ACCOUNT_ID, TEST_CREATED_BY).await().indefinitely();

        Long deleted = graphService.deleteAll(TEST_GRAPH_ID).await().indefinitely();
        assertEquals(3L, deleted);

        List<PipelineGraphEntity> versions = graphService.findAllVersions(TEST_GRAPH_ID)
                .await().indefinitely();
        assertTrue(versions.isEmpty());
    }

    /**
     * Tests converting entity to protobuf.
     */
    @Test
    void testToProto() {
        PipelineGraph original = createTestGraph(1L);
        PipelineGraphEntity entity = graphService.create(original, TEST_ACCOUNT_ID, TEST_CREATED_BY)
                .await().indefinitely();

        PipelineGraph deserialized = graphService.toProto(entity).await().indefinitely();

        assertEquals(original.getGraphId(), deserialized.getGraphId());
        assertEquals(original.getClusterId(), deserialized.getClusterId());
        assertEquals(original.getVersion(), deserialized.getVersion());
        assertEquals(original.getNodeIdsCount(), deserialized.getNodeIdsCount());
        assertEquals(original.getEdgesCount(), deserialized.getEdgesCount());
    }

    // Helper methods

    private PipelineGraph createTestGraph(Long version) {
        return createTestGraph(TEST_GRAPH_ID, version);
    }

    private PipelineGraph createTestGraph(String graphId, Long version) {
        return PipelineGraph.newBuilder()
                .setGraphId(graphId)
                .setClusterId(TEST_CLUSTER_ID)
                .setName("Test Graph")
                .setDescription("Test graph for CRUD operations")
                .setVersion(version)
                .setMode(GraphMode.GRAPH_MODE_PRODUCTION)
                .addNodeIds("node-1")
                .addNodeIds("node-2")
                .addEdges(GraphEdge.newBuilder()
                        .setEdgeId("edge-1")
                        .setFromNodeId("node-1")
                        .setToNodeId("node-2")
                        .setPriority(1)
                        .setTransportType(TransportType.TRANSPORT_TYPE_GRPC)
                        .build())
                .setCreatedAt(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond())
                        .setNanos(Instant.now().getNano())
                        .build())
                .build();
    }
}

