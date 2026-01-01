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

    @Inject
    DatasourceInstanceService datasourceInstanceService;

    @Inject
    GraphCache graphCache;

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
        LOG.debugf("CreateGraph: graph_id=%s, version=%d",
                request.getGraph().getGraphId(), request.getGraph().getVersion());

        return Panache.withTransaction(() ->
            graphService.create(request.getGraph(), request.getCreatedBy())
                .map(entity -> {
                    broadcastEvent(GraphUpdateType.GRAPH_UPDATE_TYPE_CREATED, entity);
                    return toCreateResponse(entity);
                })
        );
    }

    /**
     * Creates and immediately activates a new graph version.
     * <p>
     * This operation:
     * 1. Creates the new graph version
     * 2. Deactivates any currently active version for this graph_id
     * 3. Activates the new version
     * 4. Loads the graph into GraphCache for routing
     * 5. Note: DatasourceInstances must be created separately via CreateDatasourceInstance RPC
     */
    @Override
    public Uni<CreateAndActivateGraphResponse> createAndActivateGraph(CreateAndActivateGraphRequest request) {
        LOG.debugf("CreateAndActivateGraph: graph_id=%s, version=%d",
                request.getGraph().getGraphId(), request.getGraph().getVersion());

        // First, unload DatasourceInstances from any previously active version of this graph
        datasourceInstanceService.unloadFromCache(request.getGraph().getGraphId());

        return graphService.createAndActivate(request.getGraph(), request.getCreatedBy())
            .map(entity -> {
                // Load the graph into GraphCache
                graphCache.loadGraph(entity.toProto());

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
     * <p>
     * This operation:
     * 1. Deactivates all other versions for this graph_id
     * 2. Activates the specified version in the database
     * 3. Loads the graph into GraphCache for routing
     * 4. Loads all DatasourceInstances for this graph version into GraphCache
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

                    // First, unload DatasourceInstances from any previously active version of this graph
                    datasourceInstanceService.unloadFromCache(request.getGraphId());

                    return graphService.activate(request.getGraphId(), request.getVersion())
                        .flatMap(v -> {
                            // Load the graph into GraphCache
                            graphCache.loadGraph(entity.toProto());

                            // Load DatasourceInstances for this graph version into cache
                            return datasourceInstanceService.loadIntoCache(request.getGraphId(), request.getVersion())
                                .map(ignored -> {
                                    // Broadcast activation event
                                    entity.isActive = true;
                                    broadcastEvent(GraphUpdateType.GRAPH_UPDATE_TYPE_ACTIVATED, entity);
                                    return ActivateGraphResponse.newBuilder()
                                            .setSuccess(true)
                                            .setMessage("Graph activated: " + request.getGraphId() + " v" + request.getVersion())
                                            .build();
                                });
                        });
                })
        );
    }

    /**
     * Deactivates all versions of a graph in a cluster.
     * <p>
     * This operation:
     * 1. Deactivates all versions in the database
     * 2. Unloads all DatasourceInstances for this graph from GraphCache
     */
    @Override
    public Uni<DeactivateGraphResponse> deactivateGraph(DeactivateGraphRequest request) {
        LOG.debugf("DeactivateGraph: graph_id=%s, cluster_id=%s", request.getGraphId(), request.getClusterId());

        // Unload DatasourceInstances from GraphCache
        datasourceInstanceService.unloadFromCache(request.getGraphId());

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
    // DATASOURCE INSTANCE OPERATIONS
    // =========================================================================

    /**
     * Creates a DatasourceInstance binding a datasource to an entry node.
     */
    @Override
    public Uni<CreateDatasourceInstanceResponse> createDatasourceInstance(CreateDatasourceInstanceRequest request) {
        LOG.debugf("CreateDatasourceInstance: graph_id=%s, version=%d, datasource_id=%s, entry_node=%s",
                request.getGraphId(), request.getVersion(), request.getDatasourceId(), request.getEntryNodeId());

        return datasourceInstanceService.create(
                request.getGraphId(),
                request.getVersion(),
                request.getDatasourceId(),
                request.getEntryNodeId(),
                request.hasNodeConfig() ? request.getNodeConfig() : null,
                request.getCreatedBy())
            .map(entity -> CreateDatasourceInstanceResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("DatasourceInstance created: " + entity.datasourceInstanceId)
                    .setInstance(entity.toProto())
                    .build())
            .onFailure().recoverWithItem(ex -> {
                LOG.warnf(ex, "Failed to create DatasourceInstance");
                return CreateDatasourceInstanceResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to create DatasourceInstance: " + ex.getMessage())
                        .build();
            });
    }

    /**
     * Updates an existing DatasourceInstance's configuration.
     */
    @Override
    public Uni<UpdateDatasourceInstanceResponse> updateDatasourceInstance(UpdateDatasourceInstanceRequest request) {
        LOG.debugf("UpdateDatasourceInstance: id=%s", request.getDatasourceInstanceId());

        return datasourceInstanceService.update(
                request.getDatasourceInstanceId(),
                request.hasNodeConfig() ? request.getNodeConfig() : null)
            .map(entity -> {
                if (entity == null) {
                    return UpdateDatasourceInstanceResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("DatasourceInstance not found: " + request.getDatasourceInstanceId())
                            .build();
                }
                return UpdateDatasourceInstanceResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("DatasourceInstance updated")
                        .setInstance(entity.toProto())
                        .build();
            })
            .onFailure().recoverWithItem(ex -> {
                LOG.warnf(ex, "Failed to update DatasourceInstance");
                return UpdateDatasourceInstanceResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to update DatasourceInstance: " + ex.getMessage())
                        .build();
            });
    }

    /**
     * Deletes a DatasourceInstance.
     */
    @Override
    public Uni<DeleteDatasourceInstanceResponse> deleteDatasourceInstance(DeleteDatasourceInstanceRequest request) {
        LOG.debugf("DeleteDatasourceInstance: id=%s", request.getDatasourceInstanceId());

        return datasourceInstanceService.delete(request.getDatasourceInstanceId())
            .map(deleted -> DeleteDatasourceInstanceResponse.newBuilder()
                    .setSuccess(true)
                    .setDeleted(deleted)
                    .setMessage(deleted ? "DatasourceInstance deleted" : "DatasourceInstance not found")
                    .build())
            .onFailure().recoverWithItem(ex -> {
                LOG.warnf(ex, "Failed to delete DatasourceInstance");
                return DeleteDatasourceInstanceResponse.newBuilder()
                        .setSuccess(false)
                        .setDeleted(false)
                        .setMessage("Failed to delete DatasourceInstance: " + ex.getMessage())
                        .build();
            });
    }

    /**
     * Retrieves a DatasourceInstance by its ID.
     */
    @Override
    public Uni<GetDatasourceInstanceByIdResponse> getDatasourceInstanceById(GetDatasourceInstanceByIdRequest request) {
        LOG.debugf("GetDatasourceInstanceById: id=%s", request.getDatasourceInstanceId());

        return Panache.withSession(() ->
            datasourceInstanceService.findById(request.getDatasourceInstanceId())
                .flatMap(entity -> {
                    if (entity == null) {
                        return Uni.createFrom().item(GetDatasourceInstanceByIdResponse.newBuilder()
                                .setFound(false)
                                .build());
                    }
                    // Also check if the graph is active
                    return graphService.findByGraphIdAndVersion(entity.graphId, entity.version)
                        .map(graphEntity -> GetDatasourceInstanceByIdResponse.newBuilder()
                                .setFound(true)
                                .setInstance(entity.toProto())
                                .setGraphId(entity.graphId)
                                .setVersion(entity.version)
                                .setGraphIsActive(graphEntity != null && graphEntity.isActive)
                                .build());
                })
        );
    }

    /**
     * Lists all DatasourceInstances for a graph version.
     */
    @Override
    public Uni<ListDatasourceInstancesByGraphResponse> listDatasourceInstancesByGraph(ListDatasourceInstancesByGraphRequest request) {
        LOG.debugf("ListDatasourceInstancesByGraph: graph_id=%s, version=%d", request.getGraphId(), request.getVersion());

        Long version = request.getVersion() > 0 ? request.getVersion() : null;

        return Panache.withSession(() ->
            datasourceInstanceService.findByGraph(request.getGraphId(), version)
                .flatMap(entities -> {
                    // Determine the actual version we're returning
                    if (entities.isEmpty()) {
                        return Uni.createFrom().item(ListDatasourceInstancesByGraphResponse.newBuilder()
                                .setTotalCount(0)
                                .build());
                    }

                    long actualVersion = entities.get(0).version;
                    return graphService.findByGraphIdAndVersion(request.getGraphId(), actualVersion)
                        .map(graphEntity -> {
                            List<DatasourceInstanceInfo> infos = entities.stream()
                                    .map(this::toDatasourceInstanceInfo)
                                    .collect(Collectors.toList());
                            return ListDatasourceInstancesByGraphResponse.newBuilder()
                                    .addAllInstances(infos)
                                    .setTotalCount(infos.size())
                                    .setVersion(actualVersion)
                                    .setIsActive(graphEntity != null && graphEntity.isActive)
                                    .build();
                        });
                })
        );
    }

    /**
     * Lists all DatasourceInstances across all graphs for a datasource.
     */
    @Override
    public Uni<ListDatasourceInstancesByDatasourceResponse> listDatasourceInstancesByDatasource(ListDatasourceInstancesByDatasourceRequest request) {
        LOG.debugf("ListDatasourceInstancesByDatasource: datasource_id=%s, active_only=%s",
                request.getDatasourceId(), request.getActiveOnly());

        return Panache.withSession(() ->
            datasourceInstanceService.findByDatasource(request.getDatasourceId(), request.getActiveOnly())
                .map(entities -> {
                    List<DatasourceInstanceInfo> infos = entities.stream()
                            .map(this::toDatasourceInstanceInfo)
                            .collect(Collectors.toList());
                    return ListDatasourceInstancesByDatasourceResponse.newBuilder()
                            .addAllInstances(infos)
                            .setTotalCount(infos.size())
                            .build();
                })
        );
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

    private DatasourceInstanceInfo toDatasourceInstanceInfo(DatasourceInstanceEntity entity) {
        return DatasourceInstanceInfo.newBuilder()
                .setInstance(entity.toProto())
                .setGraphId(entity.graphId)
                .setVersion(entity.version)
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
