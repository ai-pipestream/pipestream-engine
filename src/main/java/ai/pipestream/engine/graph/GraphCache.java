package ai.pipestream.engine.graph;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.ModuleDefinition;
import ai.pipestream.config.v1.PipelineGraph;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;

/**
 * In-memory cache for the pipeline graph topology.
 * <p>
 * Stores Nodes, Edges, and Module Definitions to allow for fast lookups during the hot processing loop.
 */
@ApplicationScoped
public class GraphCache {

    /** Logger for this service class. */
    private static final Logger LOG = Logger.getLogger(GraphCache.class);

    /** In-memory cache mapping node IDs to their GraphNode definitions. */
    private final Map<String, GraphNode> nodeMap = new ConcurrentHashMap<>();

    /** In-memory cache mapping source node IDs to lists of their outgoing edges. */
    private final Map<String, List<GraphEdge>> outgoingEdgesMap = new ConcurrentHashMap<>();

    /** In-memory cache mapping module IDs to their ModuleDefinition configurations. */
    private final Map<String, ModuleDefinition> moduleMap = new ConcurrentHashMap<>();

    /** In-memory cache mapping datasource IDs to their entry node IDs for intake routing. */
    private final Map<String, String> entryNodeMap = new ConcurrentHashMap<>();

    /**
     * Loads an entire PipelineGraph into the cache.
     * <p>
     * Clears existing cache and loads all nodes and edges from the graph.
     * Nodes are stored individually, and edges are grouped by source node.
     * Note: Currently simplified - assumes nodes are defined elsewhere.
     *
     * @param graph The complete pipeline graph definition to cache
     */
    public void loadGraph(PipelineGraph graph) {
        LOG.infof("Loading graph: %s (Cluster: %s)", graph.getName(), graph.getClusterId());
        // Note: In a real implementation, nodes would be fetched from a repository 
        // because the PipelineGraph proto only contains node_ids (repeated string).
        // For now, we assume nodes are put individually.
        graph.getEdgesList().forEach(this::putEdge);
    }

    /**
     * Retrieves a node definition by its ID.
     *
     * @param nodeId The unique identifier of the node
     * @return An Optional containing the node if found, empty otherwise
     */
    public Optional<GraphNode> getNode(String nodeId) {
        return Optional.ofNullable(nodeMap.get(nodeId));
    }

    /**
     * Retrieves all outgoing edges from a given node.
     *
     * @param nodeId The source node ID
     * @return A list of edges from the specified node (empty if none found)
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
     *
     * @param node The node definition to cache
     */
    public void putNode(GraphNode node) {
        nodeMap.put(node.getNodeId(), node);
    }

    /**
     * Stores a module definition in the cache.
     *
     * @param module The module definition to cache
     */
    public void putModule(ModuleDefinition module) {
        moduleMap.put(module.getModuleId(), module);
    }

    /**
     * Stores an edge definition in the cache, grouping edges by source node.
     * <p>
     * If an edge with the same ID already exists from the source node,
     * it will be replaced. Edges are sorted by priority (ascending).
     *
     * @param edge The edge definition to cache
     */
    public void putEdge(GraphEdge edge) {
        outgoingEdgesMap.compute(edge.getFromNodeId(), (k, v) -> {
            List<GraphEdge> list = (v == null) ? new ArrayList<>() : new ArrayList<>(v);
            // Replace if already exists (by edge_id)
            list.removeIf(e -> e.getEdgeId().equals(edge.getEdgeId()));
            list.add(edge);
            // Sort by priority (asc)
            list.sort((e1, e2) -> Integer.compare(e1.getPriority(), e2.getPriority()));
            return list;
        });
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
     * Removes all nodes, edges, modules, and entry node mappings.
     * Useful for reloading the graph or during testing.
     */
    public void clear() {
        nodeMap.clear();
        outgoingEdgesMap.clear();
        moduleMap.clear();
        entryNodeMap.clear();
    }
}