package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphMode;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.NodeType;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.config.v1.TransportType;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.hibernate.reactive.panache.TransactionalUniAsserter;
import io.quarkus.test.vertx.RunOnVertxContext;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for PipelineGraphService CRUD operations.
 * <p>
 * Tests require a PostgreSQL database (provided by Quarkus DevServices).
 * All operations use TransactionalUniAsserter for reactive database access.
 * Each test uses unique graph IDs to avoid conflicts between tests.
 */
@QuarkusTest
class PipelineGraphServiceTest {

    @Inject
    PipelineGraphService graphService;

    private static final String TEST_CLUSTER_ID = "test-cluster";
    private static final String TEST_CREATED_BY = "test-user";

    /**
     * Generates a unique graph ID for each test to avoid conflicts.
     */
    private String uniqueGraphId() {
        return "test-graph-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Tests creating a new graph version.
     */
    @Test
    @RunOnVertxContext
    void testCreate(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph graph = createTestGraph(graphId, 1L);

        asserter.execute(() -> graphService.create(graph, TEST_CREATED_BY));

        asserter.assertThat(() -> graphService.findByGraphIdAndVersion(graphId, 1L), (PipelineGraphEntity entity) -> {
            assertThat(entity, is(notNullValue()));
            assertThat(entity.graphId, is(graphId));
            assertThat(entity.clusterId, is(TEST_CLUSTER_ID));
            assertThat(entity.version, is(1L));
            assertThat(entity.isActive, is(false)); // Default to inactive
            assertThat(entity.createdAt, is(notNullValue()));
            assertThat(entity.createdBy, is(TEST_CREATED_BY));
            assertThat(entity.graphData, is(notNullValue()));
        });
    }

    /**
     * Tests creating and activating a graph version.
     */
    @Test
    @RunOnVertxContext
    void testCreateAndActivate(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph graph1 = createTestGraph(graphId, 1L);
        PipelineGraph graph2 = createTestGraph(graphId, 2L);

        UUID[] entity1Id = new UUID[1];

        // Create first version and activate
        asserter.execute(() -> graphService.createAndActivate(graph1, TEST_CREATED_BY)
                .invoke(entity -> entity1Id[0] = entity.id));

        asserter.assertThat(() -> graphService.findById(entity1Id[0]), (PipelineGraphEntity entity) -> {
            assertThat(entity.isActive, is(true));
        });

        // Create second version and activate - should deactivate first
        asserter.execute(() -> graphService.createAndActivate(graph2, TEST_CREATED_BY));

        // Verify first version is now inactive
        asserter.assertThat(() -> graphService.findById(entity1Id[0]), (PipelineGraphEntity entity) -> {
            assertThat(entity.isActive, is(false));
        });

        // Verify second version is active
        asserter.assertThat(() -> graphService.findActive(graphId, TEST_CLUSTER_ID), (PipelineGraphEntity entity) -> {
            assertThat(entity, is(notNullValue()));
            assertThat(entity.version, is(2L));
            assertThat(entity.isActive, is(true));
        });
    }

    /**
     * Tests finding a graph by ID.
     */
    @Test
    @RunOnVertxContext
    void testFindById(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph graph = createTestGraph(graphId, 1L);
        UUID[] entityId = new UUID[1];

        asserter.execute(() -> graphService.create(graph, TEST_CREATED_BY)
                .invoke(entity -> entityId[0] = entity.id));

        asserter.assertThat(() -> graphService.findById(entityId[0]), (PipelineGraphEntity found) -> {
            assertThat(found, is(notNullValue()));
            assertThat(found.id, is(entityId[0]));
            assertThat(found.graphId, is(graphId));
        });
    }

    /**
     * Tests finding a graph by graph_id and version.
     */
    @Test
    @RunOnVertxContext
    void testFindByGraphIdAndVersion(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph graph1 = createTestGraph(graphId, 1L);
        PipelineGraph graph2 = createTestGraph(graphId, 2L);

        asserter.execute(() -> graphService.create(graph1, TEST_CREATED_BY));
        asserter.execute(() -> graphService.create(graph2, TEST_CREATED_BY));

        asserter.assertThat(() -> graphService.findByGraphIdAndVersion(graphId, 2L), (PipelineGraphEntity found) -> {
            assertThat(found, is(notNullValue()));
            assertThat(found.version, is(2L));
        });
    }

    /**
     * Tests finding the active graph for a graph_id and cluster.
     */
    @Test
    @RunOnVertxContext
    void testFindActive(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph graph1 = createTestGraph(graphId, 1L);
        PipelineGraph graph2 = createTestGraph(graphId, 2L);

        asserter.execute(() -> graphService.create(graph1, TEST_CREATED_BY));
        asserter.execute(() -> graphService.createAndActivate(graph2, TEST_CREATED_BY));

        asserter.assertThat(() -> graphService.findActive(graphId, TEST_CLUSTER_ID), (PipelineGraphEntity active) -> {
            assertThat(active, is(notNullValue()));
            assertThat(active.version, is(2L));
            assertThat(active.isActive, is(true));
        });
    }

    /**
     * Tests finding all active graphs for a cluster.
     */
    @Test
    @RunOnVertxContext
    void testFindActiveByCluster(TransactionalUniAsserter asserter) {
        PipelineGraph graph1 = createTestGraph("graph-1", 1L);
        PipelineGraph graph2 = createTestGraph("graph-2", 1L);
        PipelineGraph graph3 = createTestGraph("graph-3", 1L);

        asserter.execute(() -> graphService.createAndActivate(graph1, TEST_CREATED_BY));
        asserter.execute(() -> graphService.createAndActivate(graph2, TEST_CREATED_BY));
        asserter.execute(() -> graphService.create(graph3, TEST_CREATED_BY)); // Inactive

        asserter.assertThat(() -> graphService.findActiveByCluster(TEST_CLUSTER_ID), (List<PipelineGraphEntity> active) -> {
            assertThat(active.size(), is(greaterThanOrEqualTo(2)));
            assertThat(active.stream().allMatch(e -> e.isActive), is(true));
        });
    }

    /**
     * Tests finding all versions of a graph.
     */
    @Test
    @RunOnVertxContext
    void testFindAllVersions(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph graph1 = createTestGraph(graphId, 1L);
        PipelineGraph graph2 = createTestGraph(graphId, 2L);
        PipelineGraph graph3 = createTestGraph(graphId, 3L);

        asserter.execute(() -> graphService.create(graph1, TEST_CREATED_BY));
        asserter.execute(() -> graphService.create(graph2, TEST_CREATED_BY));
        asserter.execute(() -> graphService.create(graph3, TEST_CREATED_BY));

        asserter.assertThat(() -> graphService.findAllVersions(graphId), (List<PipelineGraphEntity> versions) -> {
            assertThat(versions.size(), is(3));
            // Should be ordered by version DESC
            assertThat(versions.get(0).version, is(3L));
            assertThat(versions.get(1).version, is(2L));
            assertThat(versions.get(2).version, is(1L));
        });
    }

    /**
     * Tests getting the maximum version number.
     */
    @Test
    @RunOnVertxContext
    void testGetMaxVersion(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();

        asserter.assertThat(() -> graphService.getMaxVersion(graphId), (Long maxVersion) -> {
            assertThat(maxVersion, is(0L));
        });

        asserter.execute(() -> graphService.create(createTestGraph(graphId, 1L), TEST_CREATED_BY));

        asserter.assertThat(() -> graphService.getMaxVersion(graphId), (Long maxVersion) -> {
            assertThat(maxVersion, is(1L));
        });

        asserter.execute(() -> graphService.create(createTestGraph(graphId, 5L), TEST_CREATED_BY));

        asserter.assertThat(() -> graphService.getMaxVersion(graphId), (Long maxVersion) -> {
            assertThat(maxVersion, is(5L));
        });
    }

    /**
     * Tests activating a specific version.
     */
    @Test
    @RunOnVertxContext
    void testActivate(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph graph1 = createTestGraph(graphId, 1L);
        PipelineGraph graph2 = createTestGraph(graphId, 2L);

        UUID[] entity1Id = new UUID[1];
        UUID[] entity2Id = new UUID[1];

        asserter.execute(() -> graphService.createAndActivate(graph1, TEST_CREATED_BY)
                .invoke(entity -> entity1Id[0] = entity.id));
        asserter.execute(() -> graphService.create(graph2, TEST_CREATED_BY)
                .invoke(entity -> entity2Id[0] = entity.id));

        asserter.assertThat(() -> graphService.findById(entity1Id[0]), (PipelineGraphEntity entity) -> {
            assertThat(entity.isActive, is(true));
        });
        asserter.assertThat(() -> graphService.findById(entity2Id[0]), (PipelineGraphEntity entity) -> {
            assertThat(entity.isActive, is(false));
        });

        // Activate version 2
        asserter.execute(() -> graphService.activate(graphId, 2L));

        // Verify both are updated
        asserter.assertThat(() -> graphService.findById(entity1Id[0]), (PipelineGraphEntity entity) -> {
            assertThat(entity.isActive, is(false));
        });
        asserter.assertThat(() -> graphService.findById(entity2Id[0]), entity -> {
            assertThat(entity.isActive, is(true));
        });
    }

    /**
     * Tests deactivating all versions.
     */
    @Test
    @RunOnVertxContext
    void testDeactivateAll(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph graph1 = createTestGraph(graphId, 1L);
        PipelineGraph graph2 = createTestGraph(graphId, 2L);

        asserter.execute(() -> graphService.createAndActivate(graph1, TEST_CREATED_BY));
        asserter.execute(() -> graphService.create(graph2, TEST_CREATED_BY));

        asserter.execute(() -> graphService.deactivateAll(graphId, TEST_CLUSTER_ID));

        asserter.assertThat(() -> graphService.findActive(graphId, TEST_CLUSTER_ID), (PipelineGraphEntity active) -> {
            assertThat(active, is(nullValue()));
        });
    }

    /**
     * Tests deleting a specific version.
     */
    @Test
    @RunOnVertxContext
    void testDelete(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph graph = createTestGraph(graphId, 1L);
        UUID[] entityId = new UUID[1];

        asserter.execute(() -> graphService.create(graph, TEST_CREATED_BY)
                .invoke(entity -> entityId[0] = entity.id));

        asserter.execute(() -> graphService.delete(graphId, 1L));

        asserter.assertThat(() -> graphService.findById(entityId[0]), (PipelineGraphEntity found) -> {
            assertThat(found, is(nullValue()));
        });
    }

    /**
     * Tests deleting all versions of a graph.
     */
    @Test
    @RunOnVertxContext
    void testDeleteAll(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();

        asserter.execute(() -> graphService.create(createTestGraph(graphId, 1L), TEST_CREATED_BY));
        asserter.execute(() -> graphService.create(createTestGraph(graphId, 2L), TEST_CREATED_BY));
        asserter.execute(() -> graphService.create(createTestGraph(graphId, 3L), TEST_CREATED_BY));

        asserter.execute(() -> graphService.deleteAll(graphId));

        asserter.assertThat(() -> graphService.findAllVersions(graphId), (List<PipelineGraphEntity> versions) -> {
            assertThat(versions.isEmpty(), is(true));
        });
    }

    /**
     * Tests converting entity to protobuf.
     */
    @Test
    @RunOnVertxContext
    void testToProto(TransactionalUniAsserter asserter) {
        String graphId = uniqueGraphId();
        PipelineGraph original = createTestGraph(graphId, 1L);
        UUID[] entityId = new UUID[1];

        asserter.execute(() -> graphService.create(original, TEST_CREATED_BY)
                .invoke(entity -> entityId[0] = entity.id));

        asserter.assertThat(() -> graphService.findById(entityId[0])
                .chain(entity -> graphService.toProto(entity)), (PipelineGraph deserialized) -> {
            assertThat(deserialized.getGraphId(), is(original.getGraphId()));
            assertThat(deserialized.getClusterId(), is(original.getClusterId()));
            assertThat(deserialized.getVersion(), is(original.getVersion()));
            assertThat(deserialized.getNodesCount(), is(original.getNodesCount()));
            assertThat(deserialized.getEdgesCount(), is(original.getEdgesCount()));
        });
    }

    // Helper methods

    private PipelineGraph createTestGraph(String graphId, Long version) {
        return PipelineGraph.newBuilder()
                .setGraphId(graphId)
                .setClusterId(TEST_CLUSTER_ID)
                .setName("Test Graph")
                .setDescription("Test graph for CRUD operations")
                .setVersion(version)
                .setMode(GraphMode.GRAPH_MODE_PRODUCTION)
                .addNodes(GraphNode.newBuilder()
                        .setNodeId("node-1")
                        .setClusterId(TEST_CLUSTER_ID)
                        .setName("Node 1")
                        .setNodeType(NodeType.NODE_TYPE_PROCESSOR)
                        .build())
                .addNodes(GraphNode.newBuilder()
                        .setNodeId("node-2")
                        .setClusterId(TEST_CLUSTER_ID)
                        .setName("Node 2")
                        .setNodeType(NodeType.NODE_TYPE_PROCESSOR)
                        .build())
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
