package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.engine.v1.DatasourceInstance;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

/**
 * In-memory cache for the pipeline graph topology.
 * <p>
 * Stores Nodes, Edges, and Module Definitions to allow for fast lookups during the hot processing loop.
 * Uses atomic state swapping with volatile references to ensure processing threads always see
 * a consistent version of the graph during updates.
 * <p>
 * The cache is optimized for the hot processing path:
 * - O(1) node lookup by ID
 * - Pre-grouped edges by source node for fast routing
 * - Thread-safe concurrent access without blocking
 */
@ApplicationScoped
public class GraphCache {

    /** Logger for this service class. */
    private static final Logger LOG = Logger.getLogger(GraphCache.class);

    /** CDI event for notifying observers when a graph is loaded. */
    @Inject
    Event<GraphLoadedEvent> graphLoadedEvent;

    /** Current graph version - volatile ensures all threads see updates immediately. */
    private volatile long currentVersion = 0L;

    /** Current graph reference - volatile for atomic state swapping. */
    private volatile PipelineGraph currentGraph = null;

    /** In-memory cache mapping node IDs to their GraphNode definitions. */
    private volatile Map<String, GraphNode> nodeMap = new ConcurrentHashMap<>();

    /** In-memory cache mapping source node IDs to lists of their outgoing edges. */
    private volatile Map<String, List<GraphEdge>> outgoingEdgesMap = new ConcurrentHashMap<>();

    /** In-memory cache mapping module IDs to their ModuleDefinition configurations. */
    private volatile Map<String, ModuleDefinition> moduleMap = new ConcurrentHashMap<>();

    /** In-memory cache mapping datasource IDs to their entry node IDs for intake routing.
     * @deprecated Use {@link #datasourceInstanceMap} for full DatasourceInstance with Tier 2 config */
    private volatile Map<String, String> entryNodeMap = new ConcurrentHashMap<>();

    /**
     * In-memory cache mapping datasource IDs to their DatasourceInstances.
     * <p>
     * A datasource_id can have multiple DatasourceInstances across different graphs,
     * enabling multicast routing (one document → multiple entry nodes/pipelines).
     * Each DatasourceInstance contains:
     * - datasource_instance_id: Graph-versioned unique ID
     * - datasource_id: References datasource from datasource-admin (Tier 1 config owner)
     * - entry_node_id: Engine entry node for routing
     * - node_config: Tier 2 per-node configuration (optional overrides)
     */
    private volatile Map<String, List<DatasourceInstance>> datasourceInstanceMap = new ConcurrentHashMap<>();

    /**
     * Tracks which DatasourceInstance IDs belong to each graph.
     * <p>
     * Used for efficient unregistration when a graph is deactivated.
     * Key: graphId, Value: Set of datasourceInstanceIds belonging to that graph.
     */
    private volatile Map<String, java.util.Set<String>> graphToDatasourceInstanceIds = new ConcurrentHashMap<>();

    /**
     * Loads an entire PipelineGraph into the cache.
     * <p>
     * Delegates to {@link #rebuild(PipelineGraph)} for atomic state rebuilding.
     * PipelineGraph now contains embedded GraphNode definitions, which are automatically
     * extracted and cached during rebuild.
     *
     * @param graph The complete pipeline graph definition to cache
     */
    public void loadGraph(PipelineGraph graph) {
        LOG.infof("Loading graph: %s (Cluster: %s, Version: %d)", 
                graph.getName(), graph.getClusterId(), graph.getVersion());
        rebuild(graph);
    }

    /**
     * Rebuilds the cache from a complete PipelineGraph with atomic state swapping.
     * <p>
     * This method follows the pattern described in the architecture documentation:
     * 1. Extracts embedded nodes from the graph and indexes them
     * 2. Builds new index structures (nodes, edges, modules)
     * 3. Groups edges by source node and sorts by priority
     * 4. Atomically swaps all volatile references at the end
     * <p>
     * This ensures that processing threads never see a partially updated or
     * inconsistent graph state. All lookups during rebuild will continue to
     * see the previous version until the swap completes.
     * <p>
     * PipelineGraph now contains embedded GraphNode objects, so nodes are
     * automatically extracted and cached. Module definitions are still managed separately.
     *
     * @param graph The complete pipeline graph definition to cache
     */
    public void rebuild(PipelineGraph graph) {
        LOG.infof("Rebuilding graph cache: %s (Version: %d)", graph.getName(), graph.getVersion());

        // Capture previous version for event notification
        long previousVersion = this.currentVersion;

        // Extract and index embedded nodes
        Map<String, GraphNode> newNodeMap = new ConcurrentHashMap<>();
        for (GraphNode node : graph.getNodesList()) {
            String nodeId = node.getNodeId();
            if (nodeId == null || nodeId.isEmpty()) {
                LOG.warnf("Skipping node with missing node_id: %s", node.getName());
                continue;
            }
            newNodeMap.put(nodeId, node);
        }

        // Build new edge index structure (non-volatile during construction)
        Map<String, List<GraphEdge>> newOutgoingEdgesMap = new HashMap<>();

        // Index edges by source node and sort by priority
        for (GraphEdge edge : graph.getEdgesList()) {
            String fromNodeId = edge.getFromNodeId();
            if (fromNodeId == null || fromNodeId.isEmpty()) {
                LOG.warnf("Skipping edge with missing from_node_id: %s", edge.getEdgeId());
                continue;
            }

            List<GraphEdge> edgeList = newOutgoingEdgesMap.computeIfAbsent(fromNodeId, k -> new ArrayList<>());
            
            // Remove existing edge with same ID (if any)
            edgeList.removeIf(e -> e.getEdgeId().equals(edge.getEdgeId()));
            
            // Add the new edge
            edgeList.add(edge);
            
            // Sort by priority (ascending - lower priority number = higher priority)
            edgeList.sort((e1, e2) -> Integer.compare(e1.getPriority(), e2.getPriority()));
        }

        // Convert edge lists to unmodifiable for thread safety (after sorting)
        Map<String, List<GraphEdge>> unmodifiableEdgesMap = newOutgoingEdgesMap.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        e -> Collections.unmodifiableList(e.getValue())
                ));

        // Atomic state swap - all processing threads will see the new state immediately
        this.currentGraph = graph;
        this.currentVersion = graph.getVersion();
        this.nodeMap = newNodeMap;
        this.outgoingEdgesMap = unmodifiableEdgesMap;
        // Note: moduleMap is preserved (not cleared) as modules are managed separately
        // entryNodeMap is preserved as it's maintained independently

        LOG.infof("Graph cache rebuilt successfully: %d nodes, %d edge groups, %d modules, version %d",
                newNodeMap.size(), unmodifiableEdgesMap.size(), this.moduleMap.size(), graph.getVersion());

        // Fire async event to notify observers (e.g., CEL cache warmup)
        // Using fireAsync to avoid blocking the graph swap operation
        GraphLoadedEvent event = new GraphLoadedEvent(graph, previousVersion, graph.getVersion());
        graphLoadedEvent.fireAsync(event)
                .thenAccept(e -> LOG.debugf("GraphLoadedEvent processed: %s", e))
                .exceptionally(error -> {
                    LOG.warnf("Error processing GraphLoadedEvent: %s", error.getMessage());
                    return null;
                });
    }

    /**
     * Retrieves the current cached graph (reactive).
     * <p>
     * Since GraphCache uses volatile references, this lookup is thread-safe.
     * Wrapped in Uni for reactive composition.
     *
     * @return A Uni that completes with the current PipelineGraph, or null if no graph has been loaded
     */
    public Uni<PipelineGraph> getCurrentGraph() {
        return Uni.createFrom().item(() -> currentGraph);
    }

    /**
     * Retrieves the current graph version.
     *
     * @return The version number of the currently cached graph, or 0 if no graph is loaded
     */
    public long getCurrentVersion() {
        return currentVersion;
    }

    /**
     * Retrieves a node definition by its ID (reactive).
     * <p>
     * Uses the volatile nodeMap reference for thread-safe access.
     * Wrapped in Uni for reactive composition.
     *
     * @param nodeId The unique identifier of the node
     * @return A Uni that completes with an Optional containing the node if found, empty otherwise
     */
    public Uni<Optional<GraphNode>> getNode(String nodeId) {
        return Uni.createFrom().item(() -> Optional.ofNullable(nodeMap.get(nodeId)));
    }

    /**
     * Retrieves all outgoing edges from a given node (reactive).
     * <p>
     * Returns edges sorted by priority (ascending - lower number = higher priority).
     * Uses the volatile outgoingEdgesMap reference for thread-safe access.
     * Wrapped in Uni for reactive composition.
     *
     * @param nodeId The source node ID
     * @return A Uni that completes with an unmodifiable list of edges from the specified node (empty if none found)
     */
    public Uni<List<GraphEdge>> getOutgoingEdges(String nodeId) {
        return Uni.createFrom().item(() -> outgoingEdgesMap.getOrDefault(nodeId, Collections.emptyList()));
    }

    /**
     * Retrieves a module definition by its ID (reactive).
     * <p>
     * Uses the volatile moduleMap reference for thread-safe access.
     * Wrapped in Uni for reactive composition.
     *
     * @param moduleId The unique identifier of the module
     * @return A Uni that completes with an Optional containing the module definition if found, empty otherwise
     */
    public Uni<Optional<ModuleDefinition>> getModule(String moduleId) {
        return Uni.createFrom().item(() -> Optional.ofNullable(moduleMap.get(moduleId)));
    }

    /**
     * Stores a node definition in the cache.
     * <p>
     * This method allows for incremental node updates after rebuild().
     * Note: Nodes are now automatically extracted from PipelineGraph during rebuild(),
     * so this method is primarily useful for incremental updates or dynamic node additions.
     *
     * @param node The node definition to cache
     */
    public void putNode(GraphNode node) {
        // Create a new map and swap atomically for thread safety
        Map<String, GraphNode> newMap = new ConcurrentHashMap<>(this.nodeMap);
        newMap.put(node.getNodeId(), node);
        this.nodeMap = newMap;
    }

    /**
     * Stores a module definition in the cache.
     * <p>
     * Modules can be added before or after calling rebuild(). Similar to nodes,
     * module definitions are typically loaded separately from the graph structure.
     *
     * @param module The module definition to cache
     */
    public void putModule(ModuleDefinition module) {
        // Create a new map and swap atomically for thread safety
        Map<String, ModuleDefinition> newMap = new ConcurrentHashMap<>(this.moduleMap);
        newMap.put(module.getModuleId(), module);
        this.moduleMap = newMap;
    }

    /**
     * Stores an edge definition in the cache, grouping edges by source node.
     * <p>
     * This method is useful for incremental updates, but for bulk loading,
     * prefer using {@link #rebuild(PipelineGraph)} which is more efficient.
     * <p>
     * If an edge with the same ID already exists from the source node,
     * it will be replaced. Edges are sorted by priority (ascending).
     *
     * @param edge The edge definition to cache
     */
    public void putEdge(GraphEdge edge) {
        // Build new map with updated edge for atomic swap
        Map<String, List<GraphEdge>> newMap = new HashMap<>(this.outgoingEdgesMap);
        
        String fromNodeId = edge.getFromNodeId();
        if (fromNodeId == null || fromNodeId.isEmpty()) {
            LOG.warnf("Cannot add edge with missing from_node_id: %s", edge.getEdgeId());
            return;
        }
        
        List<GraphEdge> edgeList = new ArrayList<>(newMap.getOrDefault(fromNodeId, Collections.emptyList()));
        edgeList.removeIf(e -> e.getEdgeId().equals(edge.getEdgeId()));
        edgeList.add(edge);
        edgeList.sort((e1, e2) -> Integer.compare(e1.getPriority(), e2.getPriority()));
        
        newMap.put(fromNodeId, Collections.unmodifiableList(edgeList));
        this.outgoingEdgesMap = newMap;
    }

    /**
     * Registers an entry node for a specific datasource.
     * <p>
     * Entry nodes are the starting points for document intake from
     * different data sources (e.g., Kafka topics, direct API calls).
     *
     * @param datasourceId The identifier of the data source
     * @param entryNodeId The ID of the node that handles intake for this datasource
     */
    public void registerEntryNode(String datasourceId, String entryNodeId) {
        entryNodeMap.put(datasourceId, entryNodeId);
    }

    /**
     * Retrieves the entry node ID for a specific datasource (reactive).
     * <p>
     * Uses the volatile entryNodeMap reference for thread-safe access.
     * Wrapped in Uni for reactive composition.
     * <p>
     * Note: This method returns only the first entry node. For multicast scenarios
     * where multiple DatasourceInstances exist, use {@link #getDatasourceInstances(String)}.
     *
     * @param datasourceId The identifier of the data source
     * @return A Uni that completes with an Optional containing the entry node ID if registered, empty otherwise
     * @deprecated Use {@link #getDatasourceInstances(String)} for full DatasourceInstance with Tier 2 config
     */
    @Deprecated
    public Uni<Optional<String>> getEntryNodeId(String datasourceId) {
        // First try the new datasourceInstanceMap
        List<DatasourceInstance> instances = datasourceInstanceMap.get(datasourceId);
        if (instances != null && !instances.isEmpty()) {
            return Uni.createFrom().item(Optional.of(instances.get(0).getEntryNodeId()));
        }
        // Fall back to legacy entryNodeMap for backward compatibility
        return Uni.createFrom().item(() -> Optional.ofNullable(entryNodeMap.get(datasourceId)));
    }

    /**
     * Retrieves all DatasourceInstances for a specific datasource (reactive).
     * <p>
     * A datasource_id can have multiple DatasourceInstances across different graphs,
     * enabling multicast routing (one document → multiple pipelines).
     * <p>
     * Uses the volatile datasourceInstanceMap reference for thread-safe access.
     * Wrapped in Uni for reactive composition.
     *
     * @param datasourceId The identifier of the data source
     * @return A Uni that completes with a list of DatasourceInstances (empty if none registered)
     */
    public Uni<List<DatasourceInstance>> getDatasourceInstances(String datasourceId) {
        return Uni.createFrom().item(() ->
            datasourceInstanceMap.getOrDefault(datasourceId, Collections.emptyList())
        );
    }

    /**
     * Retrieves a single DatasourceInstance for config resolution (reactive).
     * <p>
     * When multiple DatasourceInstances exist for a datasource_id, returns the first one found.
     * This is used for Tier 2 config resolution by intake - actual routing during IntakeHandoff
     * will use all matching instances for multicast.
     *
     * @param datasourceId The identifier of the data source
     * @return A Uni that completes with an Optional containing the DatasourceInstance if found, empty otherwise
     */
    public Uni<Optional<DatasourceInstance>> getDatasourceInstance(String datasourceId) {
        return Uni.createFrom().item(() -> {
            List<DatasourceInstance> instances = datasourceInstanceMap.get(datasourceId);
            if (instances != null && !instances.isEmpty()) {
                return Optional.of(instances.get(0));
            }
            return Optional.empty();
        });
    }

    /**
     * Registers a DatasourceInstance for a specific datasource.
     * <p>
     * Called when a graph containing a DatasourceInstance is activated.
     * Multiple DatasourceInstances can be registered for the same datasource_id
     * (from different graphs), enabling multicast routing.
     *
     * @param instance The DatasourceInstance to register
     * @param graphId The graph that owns this DatasourceInstance (for unregistration tracking)
     */
    public void registerDatasourceInstance(DatasourceInstance instance, String graphId) {
        String datasourceId = instance.getDatasourceId();
        String instanceId = instance.getDatasourceInstanceId();
        LOG.debugf("Registering DatasourceInstance: %s → %s (entry: %s, graph: %s)",
                instanceId, datasourceId, instance.getEntryNodeId(), graphId);

        // Atomic update with copy-on-write semantics
        Map<String, List<DatasourceInstance>> newMap = new ConcurrentHashMap<>(this.datasourceInstanceMap);
        List<DatasourceInstance> instanceList = new ArrayList<>(
            newMap.getOrDefault(datasourceId, Collections.emptyList())
        );

        // Remove existing instance with same ID (if any) to support updates
        instanceList.removeIf(i -> i.getDatasourceInstanceId().equals(instanceId));
        instanceList.add(instance);

        newMap.put(datasourceId, Collections.unmodifiableList(instanceList));
        this.datasourceInstanceMap = newMap;

        // Track graph ownership for efficient unregistration
        if (graphId != null && !graphId.isEmpty()) {
            Map<String, java.util.Set<String>> newGraphMap = new ConcurrentHashMap<>(this.graphToDatasourceInstanceIds);
            java.util.Set<String> instanceIds = new java.util.HashSet<>(
                newGraphMap.getOrDefault(graphId, Collections.emptySet())
            );
            instanceIds.add(instanceId);
            newGraphMap.put(graphId, Collections.unmodifiableSet(instanceIds));
            this.graphToDatasourceInstanceIds = newGraphMap;
        }

        // Also update legacy entryNodeMap for backward compatibility
        this.entryNodeMap.put(datasourceId, instance.getEntryNodeId());

        LOG.infof("Registered DatasourceInstance: %s → entry node %s (total instances for datasource: %d)",
                datasourceId, instance.getEntryNodeId(), instanceList.size());
    }

    /**
     * Registers multiple DatasourceInstances (typically from a graph activation).
     * <p>
     * More efficient than calling registerDatasourceInstance repeatedly
     * as it performs a single atomic swap.
     *
     * @param instances The DatasourceInstances to register
     * @param graphId The graph that owns these DatasourceInstances (for unregistration tracking)
     */
    public void registerDatasourceInstances(List<DatasourceInstance> instances, String graphId) {
        if (instances == null || instances.isEmpty()) {
            return;
        }

        LOG.debugf("Registering %d DatasourceInstances for graph %s", instances.size(), graphId);

        // Build new maps with all instances
        Map<String, List<DatasourceInstance>> newMap = new ConcurrentHashMap<>(this.datasourceInstanceMap);
        Map<String, String> newEntryMap = new ConcurrentHashMap<>(this.entryNodeMap);
        Map<String, java.util.Set<String>> newGraphMap = new ConcurrentHashMap<>(this.graphToDatasourceInstanceIds);

        java.util.Set<String> graphInstanceIds = new java.util.HashSet<>(
            newGraphMap.getOrDefault(graphId, Collections.emptySet())
        );

        for (DatasourceInstance instance : instances) {
            String datasourceId = instance.getDatasourceId();
            String instanceId = instance.getDatasourceInstanceId();

            List<DatasourceInstance> instanceList = new ArrayList<>(
                newMap.getOrDefault(datasourceId, Collections.emptyList())
            );

            // Remove existing instance with same ID (if any)
            instanceList.removeIf(i -> i.getDatasourceInstanceId().equals(instanceId));
            instanceList.add(instance);

            newMap.put(datasourceId, Collections.unmodifiableList(instanceList));
            newEntryMap.put(datasourceId, instance.getEntryNodeId());

            // Track graph ownership
            if (graphId != null && !graphId.isEmpty()) {
                graphInstanceIds.add(instanceId);
            }
        }

        // Track graph ownership
        if (graphId != null && !graphId.isEmpty()) {
            newGraphMap.put(graphId, Collections.unmodifiableSet(graphInstanceIds));
        }

        // Atomic swap
        this.datasourceInstanceMap = newMap;
        this.entryNodeMap = newEntryMap;
        this.graphToDatasourceInstanceIds = newGraphMap;

        LOG.infof("Registered %d DatasourceInstances for graph %s across %d datasources",
                instances.size(), graphId, instances.stream().map(DatasourceInstance::getDatasourceId).distinct().count());
    }

    /**
     * Unregisters a DatasourceInstance by its ID.
     * <p>
     * Called when a graph containing the DatasourceInstance is deactivated.
     *
     * @param datasourceInstanceId The unique ID of the DatasourceInstance to unregister
     */
    public void unregisterDatasourceInstance(String datasourceInstanceId) {
        LOG.debugf("Unregistering DatasourceInstance: %s", datasourceInstanceId);

        // Find and remove the instance
        Map<String, List<DatasourceInstance>> newMap = new ConcurrentHashMap<>();

        for (Map.Entry<String, List<DatasourceInstance>> entry : this.datasourceInstanceMap.entrySet()) {
            List<DatasourceInstance> filtered = entry.getValue().stream()
                .filter(i -> !i.getDatasourceInstanceId().equals(datasourceInstanceId))
                .collect(Collectors.toList());

            if (!filtered.isEmpty()) {
                newMap.put(entry.getKey(), Collections.unmodifiableList(filtered));
            }
        }

        this.datasourceInstanceMap = newMap;
        LOG.infof("Unregistered DatasourceInstance: %s", datasourceInstanceId);
    }

    /**
     * Unregisters all DatasourceInstances for a specific graph.
     * <p>
     * Called when a graph is deactivated to remove all its DatasourceInstance bindings.
     * Uses explicit graph ownership tracking (not string prefix matching).
     *
     * @param graphId The graph ID whose DatasourceInstances should be unregistered
     */
    public void unregisterDatasourceInstancesByGraph(String graphId) {
        LOG.debugf("Unregistering all DatasourceInstances for graph: %s", graphId);

        // Get the set of instance IDs belonging to this graph
        java.util.Set<String> instanceIdsToRemove = this.graphToDatasourceInstanceIds.get(graphId);
        if (instanceIdsToRemove == null || instanceIdsToRemove.isEmpty()) {
            LOG.debugf("No DatasourceInstances registered for graph: %s", graphId);
            return;
        }

        // Remove all instances belonging to this graph
        Map<String, List<DatasourceInstance>> newMap = new ConcurrentHashMap<>();
        int removedCount = 0;

        for (Map.Entry<String, List<DatasourceInstance>> entry : this.datasourceInstanceMap.entrySet()) {
            List<DatasourceInstance> filtered = entry.getValue().stream()
                .filter(i -> !instanceIdsToRemove.contains(i.getDatasourceInstanceId()))
                .collect(Collectors.toList());

            removedCount += entry.getValue().size() - filtered.size();

            if (!filtered.isEmpty()) {
                newMap.put(entry.getKey(), Collections.unmodifiableList(filtered));
            }
        }

        // Remove the graph from the tracking map
        Map<String, java.util.Set<String>> newGraphMap = new ConcurrentHashMap<>(this.graphToDatasourceInstanceIds);
        newGraphMap.remove(graphId);

        // Atomic swap
        this.datasourceInstanceMap = newMap;
        this.graphToDatasourceInstanceIds = newGraphMap;

        LOG.infof("Unregistered %d DatasourceInstances for graph: %s", removedCount, graphId);
    }
    
    /**
     * Clears all cached graph data.
     * <p>
     * Atomically clears all cache structures. Useful for reloading the graph or during testing.
     * Processing threads will see the cleared state immediately after this call.
     */
    public void clear() {
        this.currentGraph = null;
        this.currentVersion = 0L;
        this.nodeMap = new ConcurrentHashMap<>();
        this.outgoingEdgesMap = new ConcurrentHashMap<>();
        this.moduleMap = new ConcurrentHashMap<>();
        this.entryNodeMap = new ConcurrentHashMap<>();
        this.datasourceInstanceMap = new ConcurrentHashMap<>();
        this.graphToDatasourceInstanceIds = new ConcurrentHashMap<>();
        LOG.info("Graph cache cleared");
    }
}