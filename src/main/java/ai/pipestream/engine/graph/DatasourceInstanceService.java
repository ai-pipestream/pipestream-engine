package ai.pipestream.engine.graph;

import ai.pipestream.engine.v1.DatasourceInstance;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Service for managing DatasourceInstance CRUD operations in PostgreSQL.
 * <p>
 * Provides reactive methods for creating, reading, updating, and deleting
 * DatasourceInstance bindings. Also handles GraphCache synchronization
 * when instances are modified for active graphs.
 * <p>
 * All operations are fully reactive using Mutiny Uni for non-blocking database access.
 */
@ApplicationScoped
public class DatasourceInstanceService {

    private static final Logger LOG = Logger.getLogger(DatasourceInstanceService.class);

    @Inject
    GraphCache graphCache;

    @Inject
    PipelineGraphService graphService;

    /**
     * Creates a new DatasourceInstance binding.
     * <p>
     * Validates that the graph exists and the entry_node_id is valid,
     * then persists the instance. If the graph is active, also registers
     * the instance in GraphCache for immediate routing.
     *
     * @param graphId Graph identifier
     * @param version Graph version
     * @param datasourceId Datasource ID from datasource-admin
     * @param entryNodeId Entry node where documents enter the pipeline
     * @param nodeConfig Optional Tier 2 configuration
     * @param createdBy User/service creating this instance
     * @return A Uni containing the created entity
     */
    public Uni<DatasourceInstanceEntity> create(
            String graphId,
            long version,
            String datasourceId,
            String entryNodeId,
            DatasourceInstance.NodeConfig nodeConfig,
            String createdBy) {

        LOG.debugf("Creating DatasourceInstance: graph=%s:%d, datasource=%s, entry=%s",
            graphId, version, datasourceId, entryNodeId);

        return Panache.withTransaction(() ->
            // First verify the graph exists
            graphService.findByGraphIdAndVersion(graphId, version)
                .flatMap(graphEntity -> {
                    if (graphEntity == null) {
                        return Uni.createFrom().failure(
                            new IllegalArgumentException("Graph not found: " + graphId + ":" + version));
                    }

                    // TODO: Validate that entry_node_id exists in the graph
                    // This would require parsing the graph JSON and checking nodes

                    // Create and persist the entity
                    DatasourceInstanceEntity entity = DatasourceInstanceEntity.create(
                        graphId, version, datasourceId, entryNodeId, nodeConfig, createdBy);

                    return entity.<DatasourceInstanceEntity>persist()
                        .invoke(persisted -> {
                            // If graph is active, register in GraphCache
                            if (graphEntity.isActive) {
                                DatasourceInstance proto = persisted.toProto();
                                graphCache.registerDatasourceInstance(proto, graphId);
                                LOG.infof("Registered DatasourceInstance in active GraphCache: %s",
                                    persisted.datasourceInstanceId);
                            }
                        });
                })
        );
    }

    /**
     * Updates an existing DatasourceInstance's node_config.
     *
     * @param datasourceInstanceId Unique instance ID
     * @param nodeConfig New Tier 2 configuration
     * @return A Uni containing the updated entity, or null if not found
     */
    public Uni<DatasourceInstanceEntity> update(
            String datasourceInstanceId,
            DatasourceInstance.NodeConfig nodeConfig) {

        LOG.debugf("Updating DatasourceInstance: %s", datasourceInstanceId);

        return Panache.withTransaction(() ->
            findById(datasourceInstanceId)
                .flatMap(entity -> {
                    if (entity == null) {
                        return Uni.createFrom().nullItem();
                    }

                    entity.updateNodeConfig(nodeConfig);

                    return entity.<DatasourceInstanceEntity>persist()
                        .invoke(updated -> {
                            // Check if graph is active and update GraphCache
                            graphService.findByGraphIdAndVersion(entity.graphId, entity.version)
                                .subscribe().with(
                                    graphEntity -> {
                                        if (graphEntity != null && graphEntity.isActive) {
                                            DatasourceInstance proto = updated.toProto();
                                            graphCache.registerDatasourceInstance(proto, entity.graphId);
                                            LOG.infof("Updated DatasourceInstance in active GraphCache: %s",
                                                updated.datasourceInstanceId);
                                        }
                                    },
                                    error -> LOG.warnf("Failed to check graph status: %s", error.getMessage())
                                );
                        });
                })
        );
    }

    /**
     * Deletes a DatasourceInstance by its ID.
     *
     * @param datasourceInstanceId Unique instance ID
     * @return A Uni that completes with true if deleted, false if not found
     */
    public Uni<Boolean> delete(String datasourceInstanceId) {
        LOG.debugf("Deleting DatasourceInstance: %s", datasourceInstanceId);

        return Panache.withTransaction(() ->
            findById(datasourceInstanceId)
                .flatMap(entity -> {
                    if (entity == null) {
                        return Uni.createFrom().item(false);
                    }

                    // Unregister from GraphCache first
                    graphCache.unregisterDatasourceInstance(datasourceInstanceId);

                    return DatasourceInstanceEntity.delete(
                        "datasourceInstanceId = ?1", datasourceInstanceId)
                        .map(count -> count > 0);
                })
        );
    }

    /**
     * Retrieves a DatasourceInstance by its unique ID.
     *
     * @param datasourceInstanceId Unique instance ID
     * @return A Uni containing the entity if found, or null
     */
    public Uni<DatasourceInstanceEntity> findById(String datasourceInstanceId) {
        return DatasourceInstanceEntity.find(
            "datasourceInstanceId = ?1", datasourceInstanceId).firstResult();
    }

    /**
     * Lists all DatasourceInstances for a graph version.
     *
     * @param graphId Graph identifier
     * @param version Graph version (if null, uses active version)
     * @return A Uni containing the list of instances
     */
    public Uni<List<DatasourceInstanceEntity>> findByGraph(String graphId, Long version) {
        if (version != null && version > 0) {
            return DatasourceInstanceEntity.find(
                "graphId = ?1 AND version = ?2", graphId, version).list();
        } else {
            // Find active version first, then get instances
            return graphService.findActive(graphId, null)
                .flatMap(graphEntity -> {
                    if (graphEntity == null) {
                        return Uni.createFrom().item(List.of());
                    }
                    return DatasourceInstanceEntity.find(
                        "graphId = ?1 AND version = ?2", graphId, graphEntity.version).list();
                });
        }
    }

    /**
     * Lists all DatasourceInstances for a datasource across all graphs.
     *
     * @param datasourceId Datasource ID from datasource-admin
     * @param activeOnly If true, only return instances from active graphs
     * @return A Uni containing the list of instances
     */
    public Uni<List<DatasourceInstanceEntity>> findByDatasource(String datasourceId, boolean activeOnly) {
        if (activeOnly) {
            // Join with pipeline_graphs to filter by active status
            // For now, fetch all and filter in memory (can optimize with native query later)
            return DatasourceInstanceEntity.<DatasourceInstanceEntity>find(
                "datasourceId = ?1", datasourceId).list()
                .flatMap(instances -> {
                    // Filter to only active graphs
                    return Uni.join().all(
                        instances.stream()
                            .map(instance -> graphService.findByGraphIdAndVersion(instance.graphId, instance.version)
                                .map(graph -> graph != null && graph.isActive ? instance : null))
                            .toList()
                    ).andCollectFailures()
                        .map(results -> results.stream()
                            .filter(instance -> instance != null)
                            .map(obj -> (DatasourceInstanceEntity) obj)
                            .toList());
                });
        } else {
            return DatasourceInstanceEntity.find("datasourceId = ?1", datasourceId).list();
        }
    }

    /**
     * Deletes all DatasourceInstances for a graph version.
     * <p>
     * Called when a graph version is deleted.
     *
     * @param graphId Graph identifier
     * @param version Graph version
     * @return A Uni containing the number of deleted instances
     */
    public Uni<Long> deleteByGraph(String graphId, long version) {
        LOG.debugf("Deleting all DatasourceInstances for graph: %s:%d", graphId, version);

        // Unregister from GraphCache first
        graphCache.unregisterDatasourceInstancesByGraph(graphId);

        return DatasourceInstanceEntity.delete(
            "graphId = ?1 AND version = ?2", graphId, version);
    }

    /**
     * Loads all DatasourceInstances for an active graph into GraphCache.
     * <p>
     * Called when a graph is activated.
     *
     * @param graphId Graph identifier
     * @param version Graph version
     * @return A Uni that completes when all instances are loaded
     */
    public Uni<Void> loadIntoCache(String graphId, long version) {
        LOG.debugf("Loading DatasourceInstances into cache for graph: %s:%d", graphId, version);

        return findByGraph(graphId, version)
            .invoke(instances -> {
                List<DatasourceInstance> protos = instances.stream()
                    .map(DatasourceInstanceEntity::toProto)
                    .toList();
                graphCache.registerDatasourceInstances(protos, graphId);
                LOG.infof("Loaded %d DatasourceInstances into cache for graph %s:%d",
                    protos.size(), graphId, version);
            })
            .replaceWithVoid();
    }

    /**
     * Unloads all DatasourceInstances for a graph from GraphCache.
     * <p>
     * Called when a graph is deactivated.
     *
     * @param graphId Graph identifier
     */
    public void unloadFromCache(String graphId) {
        LOG.debugf("Unloading DatasourceInstances from cache for graph: %s", graphId);
        graphCache.unregisterDatasourceInstancesByGraph(graphId);
    }
}
