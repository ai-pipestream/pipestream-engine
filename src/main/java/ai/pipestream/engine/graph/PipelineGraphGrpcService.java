package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.engine.v1.*;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * gRPC service implementation for managing pipeline graphs.
 * <p>
 * This service exposes the {@link PipelineGraphService} functionality over gRPC,
 * enabling frontend applications and other services to perform graph CRUD operations,
 * activation management, and subscribe to real-time updates.
 * <p>
 * All operations are fully reactive using Mutiny for non-blocking database access.
 * The service wraps the internal {@link PipelineGraphService} and handles:
 * <ul>
 *   <li>Request/response mapping between gRPC messages and internal entities</li>
 *   <li>Transaction management for multi-step operations</li>
 *   <li>Real-time event broadcasting for graph updates</li>
 * </ul>
 *
 * @see PipelineGraphService
 * @see ai.pipestream.config.v1.PipelineGraph
 */
@GrpcService
public class PipelineGraphGrpcService extends MutinyPipelineGraphServiceGrpc.PipelineGraphServiceImplBase {

    private static final Logger LOG = Logger.getLogger(PipelineGraphGrpcService.class);

    @Inject
    PipelineGraphService graphService;

    /** Broadcast processor for real-time graph update events. */
    private final BroadcastProcessor<SubscribeToGraphUpdatesResponse> updateBroadcaster = BroadcastProcessor.create();

    // =========================================================================
    // CREATE OPERATIONS
    // =========================================================================

    /**
     * Creates a new graph version in an inactive state.
     */
    @Override
    public Uni<CreateGraphResponse> createGraph(CreateGraphRequest request) {
        LOG.debugf("CreateGraph: graph_id=%s, version=%d, account_id=%s",
                request.getGraph().getGraphId(), request.getGraph().getVersion(), request.getAccountId());

        return Panache.withTransaction(() ->
            graphService.create(request.getGraph(), request.getAccountId(), request.getCreatedBy())
                .map(entity -> {
                    broadcastEvent(GraphUpdateType.GRAPH_UPDATE_TYPE_CREATED, entity);
                    return toCreateResponse(entity);
                })
        );
    }

    /**
     * Creates and immediately activates a new graph version.
     */
    @Override
    public Uni<CreateAndActivateGraphResponse> createAndActivateGraph(CreateAndActivateGraphRequest request) {
        LOG.debugf("CreateAndActivateGraph: graph_id=%s, version=%d, account_id=%s",
                request.getGraph().getGraphId(), request.getGraph().getVersion(), request.getAccountId());

        return graphService.createAndActivate(request.getGraph(), request.getAccountId(), request.getCreatedBy())
            .map(entity -> {
                broadcastEvent(GraphUpdateType.GRAPH_UPDATE_TYPE_CREATED, entity);
                broadcastEvent(GraphUpdateType.GRAPH_UPDATE_TYPE_ACTIVATED, entity);
                return toCreateAndActivateResponse(entity);
            });
    }

    // =========================================================================
    // GET OPERATIONS
    // =========================================================================

    /**
     * Retrieves a graph by its unique database ID.
     */
    @Override
    public Uni<GetGraphByIdResponse> getGraphById(GetGraphByIdRequest request) {
        LOG.debugf("GetGraphById: id=%s", request.getId());

        UUID id;
        try {
            id = UUID.fromString(request.getId());
        } catch (IllegalArgumentException e) {
            return Uni.createFrom().item(GetGraphByIdResponse.newBuilder()
                    .setFound(false)
                    .build());
        }

        return Panache.withSession(() ->
            graphService.findById(id)
                .map(entity -> {
                    if (entity == null) {
                        return GetGraphByIdResponse.newBuilder()
                                .setFound(false)
                                .build();
                    }
                    return toGetByIdResponse(entity);
                })
        );
    }

    /**
     * Retrieves a specific graph version.
     */
    @Override
    public Uni<GetGraphByVersionResponse> getGraphByVersion(GetGraphByVersionRequest request) {
        LOG.debugf("GetGraphByVersion: graph_id=%s, version=%d", request.getGraphId(), request.getVersion());

        return Panache.withSession(() ->
            graphService.findByGraphIdAndVersion(request.getGraphId(), request.getVersion())
                .map(entity -> {
                    if (entity == null) {
                        return GetGraphByVersionResponse.newBuilder()
                                .setFound(false)
                                .build();
                    }
                    return toGetByVersionResponse(entity);
                })
        );
    }

    /**
     * Retrieves the currently active graph for a graph_id and cluster.
     */
    @Override
    public Uni<GetActiveGraphResponse> getActiveGraph(GetActiveGraphRequest request) {
        LOG.debugf("GetActiveGraph: graph_id=%s, cluster_id=%s", request.getGraphId(), request.getClusterId());

        return Panache.withSession(() ->
            graphService.findActive(request.getGraphId(), request.getClusterId())
                .map(entity -> {
                    if (entity == null) {
                        return GetActiveGraphResponse.newBuilder()
                                .setFound(false)
                                .build();
                    }
                    return toGetActiveResponse(entity);
                })
        );
    }

    // =========================================================================
    // LIST OPERATIONS
    // =========================================================================

    /**
     * Lists all versions of a graph, ordered by version descending.
     */
    @Override
    public Uni<ListGraphVersionsResponse> listGraphVersions(ListGraphVersionsRequest request) {
        LOG.debugf("ListGraphVersions: graph_id=%s", request.getGraphId());

        return Panache.withSession(() ->
            graphService.findAllVersions(request.getGraphId())
                .map(entities -> {
                    List<GraphVersionInfo> versions = entities.stream()
                            .map(this::toVersionInfo)
                            .collect(Collectors.toList());
                    return ListGraphVersionsResponse.newBuilder()
                            .addAllVersions(versions)
                            .setTotalCount(versions.size())
                            .build();
                })
        );
    }

    /**
     * Lists all active graphs for a cluster.
     */
    @Override
    public Uni<ListActiveGraphsByClusterResponse> listActiveGraphsByCluster(ListActiveGraphsByClusterRequest request) {
        LOG.debugf("ListActiveGraphsByCluster: cluster_id=%s", request.getClusterId());

        return Panache.withSession(() ->
            graphService.findActiveByCluster(request.getClusterId())
                .map(entities -> {
                    List<GraphVersionInfo> graphs = entities.stream()
                            .map(this::toVersionInfo)
                            .collect(Collectors.toList());
                    return ListActiveGraphsByClusterResponse.newBuilder()
                            .addAllGraphs(graphs)
                            .setTotalCount(graphs.size())
                            .build();
                })
        );
    }

    // =========================================================================
    // ACTIVATION OPERATIONS
    // =========================================================================

    /**
     * Activates a specific graph version.
     */
    @Override
    public Uni<ActivateGraphResponse> activateGraph(ActivateGraphRequest request) {
        LOG.debugf("ActivateGraph: graph_id=%s, version=%d", request.getGraphId(), request.getVersion());

        return Panache.withSession(() ->
            graphService.findByGraphIdAndVersion(request.getGraphId(), request.getVersion())
                .flatMap(entity -> {
                    if (entity == null) {
                        return Uni.createFrom().item(ActivateGraphResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Graph version not found: " + request.getGraphId() + " v" + request.getVersion())
                                .build());
                    }
                    return graphService.activate(request.getGraphId(), request.getVersion())
                        .map(v -> {
                            // Broadcast deactivation for old version (if any) and activation for new
                            entity.isActive = true;
                            broadcastEvent(GraphUpdateType.GRAPH_UPDATE_TYPE_ACTIVATED, entity);
                            return ActivateGraphResponse.newBuilder()
                                    .setSuccess(true)
                                    .setMessage("Graph activated: " + request.getGraphId() + " v" + request.getVersion())
                                    .build();
                        });
                })
        );
    }

    /**
     * Deactivates all versions of a graph in a cluster.
     */
    @Override
    public Uni<DeactivateGraphResponse> deactivateGraph(DeactivateGraphRequest request) {
        LOG.debugf("DeactivateGraph: graph_id=%s, cluster_id=%s", request.getGraphId(), request.getClusterId());

        return Panache.withTransaction(() ->
            graphService.deactivateAll(request.getGraphId(), request.getClusterId())
                .map(count -> DeactivateGraphResponse.newBuilder()
                        .setSuccess(true)
                        .setDeactivatedCount(count)
                        .build())
        );
    }

    // =========================================================================
    // DELETE OPERATIONS
    // =========================================================================

    /**
     * Deletes a specific graph version.
     */
    @Override
    public Uni<DeleteGraphVersionResponse> deleteGraphVersion(DeleteGraphVersionRequest request) {
        LOG.debugf("DeleteGraphVersion: graph_id=%s, version=%d", request.getGraphId(), request.getVersion());

        return Panache.withTransaction(() ->
            graphService.delete(request.getGraphId(), request.getVersion())
                .map(deleted -> {
                    if (deleted) {
                        broadcastDeleteEvent(request.getGraphId(), request.getVersion());
                    }
                    return DeleteGraphVersionResponse.newBuilder()
                            .setSuccess(true)
                            .setDeleted(deleted)
                            .build();
                })
        );
    }

    /**
     * Deletes all versions of a graph.
     */
    @Override
    public Uni<DeleteAllGraphVersionsResponse> deleteAllGraphVersions(DeleteAllGraphVersionsRequest request) {
        LOG.debugf("DeleteAllGraphVersions: graph_id=%s", request.getGraphId());

        return Panache.withTransaction(() ->
            graphService.deleteAll(request.getGraphId())
                .map(count -> {
                    if (count > 0) {
                        broadcastDeleteEvent(request.getGraphId(), -1); // -1 indicates all versions
                    }
                    return DeleteAllGraphVersionsResponse.newBuilder()
                            .setSuccess(true)
                            .setDeletedCount(count)
                            .build();
                })
        );
    }

    // =========================================================================
    // VERSION UTILITIES
    // =========================================================================

    /**
     * Gets the maximum version number for a graph.
     */
    @Override
    public Uni<GetMaxVersionResponse> getMaxVersion(GetMaxVersionRequest request) {
        LOG.debugf("GetMaxVersion: graph_id=%s", request.getGraphId());

        return Panache.withSession(() ->
            graphService.getMaxVersion(request.getGraphId())
                .map(maxVersion -> GetMaxVersionResponse.newBuilder()
                        .setMaxVersion(maxVersion)
                        .build())
        );
    }

    // =========================================================================
    // REAL-TIME SUBSCRIPTIONS
    // =========================================================================

    /**
     * Subscribes to real-time graph update notifications.
     */
    @Override
    public Multi<SubscribeToGraphUpdatesResponse> subscribeToGraphUpdates(SubscribeToGraphUpdatesRequest request) {
        LOG.debugf("SubscribeToGraphUpdates: cluster_id=%s, graph_id=%s",
                request.getClusterId(), request.getGraphId());

        return updateBroadcaster
            .filter(event -> matchesFilter(event, request));
    }

    // =========================================================================
    // HELPER METHODS
    // =========================================================================

    private CreateGraphResponse toCreateResponse(PipelineGraphEntity entity) {
        return CreateGraphResponse.newBuilder()
                .setGraph(entity.toProto())
                .setId(entity.id.toString())
                .setIsActive(entity.isActive)
                .setCreatedAt(toTimestamp(entity.createdAt))
                .build();
    }

    private CreateAndActivateGraphResponse toCreateAndActivateResponse(PipelineGraphEntity entity) {
        return CreateAndActivateGraphResponse.newBuilder()
                .setGraph(entity.toProto())
                .setId(entity.id.toString())
                .setIsActive(entity.isActive)
                .setCreatedAt(toTimestamp(entity.createdAt))
                .build();
    }

    private GetGraphByIdResponse toGetByIdResponse(PipelineGraphEntity entity) {
        return GetGraphByIdResponse.newBuilder()
                .setGraph(entity.toProto())
                .setId(entity.id.toString())
                .setIsActive(entity.isActive)
                .setAccountId(entity.accountId != null ? entity.accountId : "")
                .setCreatedBy(entity.createdBy != null ? entity.createdBy : "")
                .setCreatedAt(toTimestamp(entity.createdAt))
                .setFound(true)
                .build();
    }

    private GetGraphByVersionResponse toGetByVersionResponse(PipelineGraphEntity entity) {
        return GetGraphByVersionResponse.newBuilder()
                .setGraph(entity.toProto())
                .setId(entity.id.toString())
                .setIsActive(entity.isActive)
                .setAccountId(entity.accountId != null ? entity.accountId : "")
                .setCreatedBy(entity.createdBy != null ? entity.createdBy : "")
                .setCreatedAt(toTimestamp(entity.createdAt))
                .setFound(true)
                .build();
    }

    private GetActiveGraphResponse toGetActiveResponse(PipelineGraphEntity entity) {
        return GetActiveGraphResponse.newBuilder()
                .setGraph(entity.toProto())
                .setId(entity.id.toString())
                .setIsActive(entity.isActive)
                .setAccountId(entity.accountId != null ? entity.accountId : "")
                .setCreatedBy(entity.createdBy != null ? entity.createdBy : "")
                .setCreatedAt(toTimestamp(entity.createdAt))
                .setFound(true)
                .build();
    }

    private GraphVersionInfo toVersionInfo(PipelineGraphEntity entity) {
        return GraphVersionInfo.newBuilder()
                .setGraph(entity.toProto())
                .setId(entity.id.toString())
                .setIsActive(entity.isActive)
                .setCreatedAt(toTimestamp(entity.createdAt))
                .setCreatedBy(entity.createdBy != null ? entity.createdBy : "")
                .build();
    }

    private Timestamp toTimestamp(Instant instant) {
        if (instant == null) {
            return Timestamp.getDefaultInstance();
        }
        return Timestamps.fromMillis(instant.toEpochMilli());
    }

    private void broadcastEvent(GraphUpdateType type, PipelineGraphEntity entity) {
        PipelineGraph graph = entity.toProto();
        SubscribeToGraphUpdatesResponse event = SubscribeToGraphUpdatesResponse.newBuilder()
                .setUpdateType(type)
                .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setGraph(graph)
                .setId(entity.id.toString())
                .setGraphId(entity.graphId)
                .setVersion(entity.version)
                .setIsActive(entity.isActive)
                .build();
        updateBroadcaster.onNext(event);
    }

    private void broadcastDeleteEvent(String graphId, long version) {
        SubscribeToGraphUpdatesResponse event = SubscribeToGraphUpdatesResponse.newBuilder()
                .setUpdateType(GraphUpdateType.GRAPH_UPDATE_TYPE_DELETED)
                .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setGraphId(graphId)
                .setVersion(version)
                .setIsActive(false)
                .build();
        updateBroadcaster.onNext(event);
    }

    private boolean matchesFilter(SubscribeToGraphUpdatesResponse event, SubscribeToGraphUpdatesRequest request) {
        // Filter by cluster_id if specified
        if (!request.getClusterId().isEmpty()) {
            if (event.hasGraph() && !event.getGraph().getClusterId().equals(request.getClusterId())) {
                return false;
            }
        }

        // Filter by graph_id if specified
        if (!request.getGraphId().isEmpty()) {
            if (!event.getGraphId().equals(request.getGraphId())) {
                return false;
            }
        }

        // Filter by update types if specified
        if (request.getUpdateTypesCount() > 0) {
            if (!request.getUpdateTypesList().contains(event.getUpdateType())) {
                return false;
            }
        }

        return true;
    }
}
