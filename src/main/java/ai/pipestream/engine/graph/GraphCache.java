package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.config.v1.PipelineGraph;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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

    /** In-memory cache mapping datasource IDs to their entry node IDs for intake routing. */
    private volatile Map<String, String> entryNodeMap = new ConcurrentHashMap<>();

    /**
     * Loads an entire PipelineGraph into the cache.
     * <p>
     * Delegates to {@link #rebuild(PipelineGraph)} for atomic state rebuilding.
     * Note: PipelineGraph only contains node IDs, not full GraphNode definitions.
     * Nodes must be added separately via {@link #putNode(GraphNode)}.
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
     * 1. Builds new index structures (nodes, edges, modules)
     * 2. Groups edges by source node and sorts by priority
     * 3. Atomically swaps all volatile references at the end
     * <p>
     * This ensures that processing threads never see a partially updated or
     * inconsistent graph state. All lookups during rebuild will continue to
     * see the previous version until the swap completes.
     * <p>
     * Note: PipelineGraph proto only contains node IDs, not full GraphNode objects.
     * Nodes should be loaded separately before or after calling rebuild.
     *
     * @param graph The complete pipeline graph definition to cache
     */
    public void rebuild(PipelineGraph graph) {
        LOG.infof("Rebuilding graph cache: %s (Version: %d)", graph.getName(), graph.getVersion());

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
        this.outgoingEdgesMap = unmodifiableEdgesMap;
        // Note: nodeMap and moduleMap are preserved (not cleared) as they may be added separately
        // entryNodeMap is preserved as it's maintained independently

        LOG.infof("Graph cache rebuilt successfully: %d nodes, %d edge groups, %d modules, version %d",
                this.nodeMap.size(), unmodifiableEdgesMap.size(), this.moduleMap.size(), graph.getVersion());
    }

    /**
     * Retrieves the current cached graph.
     *
     * @return The current PipelineGraph, or null if no graph has been loaded
     */
    public PipelineGraph getCurrentGraph() {
        return currentGraph;
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
     * Retrieves a node definition by its ID.
     * <p>
     * Uses the volatile nodeMap reference for thread-safe access.
     *
     * @param nodeId The unique identifier of the node
     * @return An Optional containing the node if found, empty otherwise
     */
    public Optional<GraphNode> getNode(String nodeId) {
        return Optional.ofNullable(nodeMap.get(nodeId));
    }

    /**
     * Retrieves all outgoing edges from a given node.
     * <p>
     * Returns edges sorted by priority (ascending - lower number = higher priority).
     * Uses the volatile outgoingEdgesMap reference for thread-safe access.
     *
     * @param nodeId The source node ID
     * @return An unmodifiable list of edges from the specified node (empty if none found)
     */
    public List<GraphEdge> getOutgoingEdges(String nodeId) {
        return outgoingEdgesMap.getOrDefault(nodeId, Collections.emptyList());
    }

    /**
     * Retrieves a module definition by its ID.
     *
     * @param moduleId The unique identifier of the module
     * @return An Optional containing the module definition if found, empty otherwise
     */
    public Optional<ModuleDefinition> getModule(String moduleId) {
        return Optional.ofNullable(moduleMap.get(moduleId));
    }

    /**
     * Stores a node definition in the cache.
     * <p>
     * Nodes can be added before or after calling rebuild(). This allows
     * PipelineGraph (which only contains node IDs) to be loaded first,
     * followed by the actual GraphNode definitions.
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
     * Retrieves the entry node ID for a specific datasource.
     *
     * @param datasourceId The identifier of the data source
     * @return An Optional containing the entry node ID if registered, empty otherwise
     */
    public Optional<String> getEntryNodeId(String datasourceId) {
        return Optional.ofNullable(entryNodeMap.get(datasourceId));
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
        LOG.info("Graph cache cleared");
    }
}