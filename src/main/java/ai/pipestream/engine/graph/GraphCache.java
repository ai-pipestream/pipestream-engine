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

    private static final Logger LOG = Logger.getLogger(GraphCache.class);

    // NodeID -> GraphNode
    private final Map<String, GraphNode> nodeMap = new ConcurrentHashMap<>();

    // FromNodeID -> List<GraphEdge>
    private final Map<String, List<GraphEdge>> outgoingEdgesMap = new ConcurrentHashMap<>();
    
    // ModuleID -> ModuleDefinition
    private final Map<String, ModuleDefinition> moduleMap = new ConcurrentHashMap<>();
    
    // DatasourceID -> EntryNodeID
    private final Map<String, String> entryNodeMap = new ConcurrentHashMap<>();

    /**
     * Loads an entire PipelineGraph into the cache.
     */
    public void loadGraph(PipelineGraph graph) {
        LOG.infof("Loading graph: %s (Cluster: %s)", graph.getName(), graph.getClusterId());
        // Note: In a real implementation, nodes would be fetched from a repository 
        // because the PipelineGraph proto only contains node_ids (repeated string).
        // For now, we assume nodes are put individually.
        graph.getEdgesList().forEach(this::putEdge);
    }

    public Optional<GraphNode> getNode(String nodeId) {
        return Optional.ofNullable(nodeMap.get(nodeId));
    }

    public List<GraphEdge> getOutgoingEdges(String nodeId) {
        return outgoingEdgesMap.getOrDefault(nodeId, Collections.emptyList());
    }

    public Optional<ModuleDefinition> getModule(String moduleId) {
        return Optional.ofNullable(moduleMap.get(moduleId));
    }

    public void putNode(GraphNode node) {
        nodeMap.put(node.getNodeId(), node);
    }

    public void putModule(ModuleDefinition module) {
        moduleMap.put(module.getModuleId(), module);
    }

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

    public void registerEntryNode(String datasourceId, String entryNodeId) {
        entryNodeMap.put(datasourceId, entryNodeId);
    }

    public Optional<String> getEntryNodeId(String datasourceId) {
        return Optional.ofNullable(entryNodeMap.get(datasourceId));
    }
    
    public void clear() {
        nodeMap.clear();
        outgoingEdgesMap.clear();
        moduleMap.clear();
        entryNodeMap.clear();
    }
}