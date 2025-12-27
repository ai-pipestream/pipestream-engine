package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.PipelineGraph;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Service for managing pipeline graph CRUD operations in PostgreSQL.
 * <p>
 * Provides reactive methods for creating, reading, updating, and querying pipeline graphs.
 * Handles version management, activation/deactivation, and graph serialization/deserialization.
 * <p>
 * All operations are fully reactive using Mutiny Uni for non-blocking database access.
 */
@ApplicationScoped
public class PipelineGraphService {

    private static final Logger LOG = Logger.getLogger(PipelineGraphService.class);

    /**
     * Creates a new graph version and persists it to the database.
     * <p>
     * The graph version is automatically set from the PipelineGraph proto.
     * The entity is created with the current timestamp and marked as inactive by default.
     *
     * @param graph The PipelineGraph protobuf to store
     * @param accountId The account identifier for multi-tenant isolation
     * @param createdBy The user/service creating this version
     * @return A Uni that completes with the persisted entity
     */
    public Uni<PipelineGraphEntity> create(PipelineGraph graph, String accountId, String createdBy) {
        LOG.debugf("Creating graph: graph_id=%s, version=%d, cluster_id=%s", 
                graph.getGraphId(), graph.getVersion(), graph.getClusterId());
        
        PipelineGraphEntity entity = PipelineGraphEntity.fromProto(graph, accountId, createdBy, false);
        return entity.persist();
    }

    /**
     * Creates and activates a new graph version.
     * <p>
     * This method deactivates all existing versions for the graph_id before activating the new one,
     * ensuring only one active version exists at a time.
     *
     * @param graph The PipelineGraph protobuf to store
     * @param accountId The account identifier for multi-tenant isolation
     * @param createdBy The user/service creating this version
     * @return A Uni that completes with the persisted and activated entity
     */
    public Uni<PipelineGraphEntity> createAndActivate(PipelineGraph graph, String accountId, String createdBy) {
        return Panache.withTransaction(() -> 
            deactivateAll(graph.getGraphId(), graph.getClusterId())
                .chain(() -> {
                    PipelineGraphEntity entity = PipelineGraphEntity.fromProto(graph, accountId, createdBy, true);
                    return entity.persist();
                })
        );
    }

    /**
     * Retrieves a graph by its unique identifier.
     *
     * @param id The UUID primary key
     * @return A Uni containing the entity if found, or null
     */
    public Uni<PipelineGraphEntity> findById(UUID id) {
        return PipelineGraphEntity.findById(id);
    }

    /**
     * Retrieves a specific graph version.
     *
     * @param graphId The logical graph identifier
     * @param version The version number
     * @return A Uni containing the entity if found, or null
     */
    public Uni<PipelineGraphEntity> findByGraphIdAndVersion(String graphId, Long version) {
        return PipelineGraphEntity.find("graphId = ?1 AND version = ?2", graphId, version).firstResult();
    }

    /**
     * Retrieves the currently active graph for a given graph_id and cluster.
     *
     * @param graphId The logical graph identifier
     * @param clusterId The cluster identifier
     * @return A Uni containing the active entity if found, or null
     */
    public Uni<PipelineGraphEntity> findActive(String graphId, String clusterId) {
        return PipelineGraphEntity.find(
                "graphId = ?1 AND clusterId = ?2 AND isActive = true", 
                graphId, clusterId
        ).firstResult();
    }

    /**
     * Retrieves all active graphs for a cluster.
     * <p>
     * Useful for engine startup to load all active graphs for its cluster.
     *
     * @param clusterId The cluster identifier
     * @return A Uni containing a list of active graph entities
     */
    public Uni<List<PipelineGraphEntity>> findActiveByCluster(String clusterId) {
        return PipelineGraphEntity.find("clusterId = ?1 AND isActive = true", clusterId).list();
    }

    /**
     * Retrieves all versions of a graph, ordered by version descending (newest first).
     *
     * @param graphId The logical graph identifier
     * @return A Uni containing a list of all graph versions
     */
    public Uni<List<PipelineGraphEntity>> findAllVersions(String graphId) {
        return PipelineGraphEntity.find("graphId = ?1 ORDER BY version DESC", graphId).list();
    }

    /**
     * Gets the maximum version number for a given graph_id.
     * <p>
     * Used for calculating the next version number when creating new versions.
     *
     * @param graphId The logical graph identifier
     * @return A Uni containing the maximum version, or 0L if no versions exist
     */
    public Uni<Long> getMaxVersion(String graphId) {
        return PipelineGraphEntity.<PipelineGraphEntity>find("graphId = ?1 ORDER BY version DESC", graphId)
                .firstResult()
                .map(entity -> entity != null ? entity.version : 0L);
    }

    /**
     * Activates a specific graph version.
     * <p>
     * Deactivates all other versions for the same graph_id before activating the target version,
     * ensuring only one active version exists at a time.
     *
     * @param graphId The logical graph identifier
     * @param version The version to activate
     * @return A Uni that completes when the activation is done
     */
    public Uni<Void> activate(String graphId, Long version) {
        return Panache.withTransaction(() ->
            deactivateAll(graphId, null)
                .chain(() -> 
                    PipelineGraphEntity.update("UPDATE PipelineGraphEntity SET isActive = true WHERE graphId = ?1 AND version = ?2", graphId, version)
                )
                .replaceWithVoid()
        );
    }

    /**
     * Deactivates all versions of a graph for a specific cluster.
     * <p>
     * If clusterId is null, deactivates all versions regardless of cluster.
     *
     * @param graphId The logical graph identifier
     * @param clusterId The cluster identifier (null to deactivate across all clusters)
     * @return A Uni that completes when deactivation is done
     */
    public Uni<Integer> deactivateAll(String graphId, String clusterId) {
        if (clusterId != null) {
            return PipelineGraphEntity.update("UPDATE PipelineGraphEntity SET isActive = false WHERE graphId = ?1 AND clusterId = ?2", graphId, clusterId);
        } else {
            return PipelineGraphEntity.update("UPDATE PipelineGraphEntity SET isActive = false WHERE graphId = ?1", graphId);
        }
    }

    /**
     * Updates an existing graph entity.
     * <p>
     * Note: Graph updates typically create new versions rather than modifying existing ones.
     * This method is provided for metadata updates (e.g., created_by, is_active).
     *
     * @param entity The entity to update
     * @return A Uni that completes when the update is done
     */
    public Uni<PipelineGraphEntity> update(PipelineGraphEntity entity) {
        return entity.persist();
    }

    /**
     * Deletes a specific graph version.
     *
     * @param graphId The logical graph identifier
     * @param version The version to delete
     * @return A Uni that completes with true if deleted, false if not found
     */
    public Uni<Boolean> delete(String graphId, Long version) {
        return PipelineGraphEntity.delete("DELETE FROM PipelineGraphEntity WHERE graphId = ?1 AND version = ?2", graphId, version)
                .map(count -> count > 0);
    }

    /**
     * Deletes all versions of a graph.
     * <p>
     * Use with caution - this permanently removes all graph history.
     *
     * @param graphId The logical graph identifier
     * @return A Uni that completes with the number of deleted versions
     */
    public Uni<Long> deleteAll(String graphId) {
        return PipelineGraphEntity.delete("DELETE FROM PipelineGraphEntity WHERE graphId = ?1", graphId);
    }

    /**
     * Converts an entity to a PipelineGraph protobuf.
     * <p>
     * Convenience method that delegates to the entity's toProto() method.
     *
     * @param entity The entity to convert
     * @return A Uni containing the deserialized PipelineGraph
     */
    public Uni<PipelineGraph> toProto(PipelineGraphEntity entity) {
        return Uni.createFrom().item(entity.toProto());
    }
}

