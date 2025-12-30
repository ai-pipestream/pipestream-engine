package ai.pipestream.engine.validation;

import ai.pipestream.config.v1.GraphEdge;
import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.engine.graph.GraphCache;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Core service for validating graph structure and node references before processing.
 * <p>
 * This service ensures that:
 * - Node IDs exist in the active graph before processing
 * - Edge targets reference valid nodes
 * - Module references are valid (modules exist in the graph cache)
 * <p>
 * Validation failures throw appropriate gRPC status exceptions with helpful error messages
 * to aid in debugging configuration errors.
 * <p>
 * This is the core validation logic service. It can be used directly by EngineV1Service
 * for internal validation, or exposed via {@link ValidationServiceImpl} (gRPC service)
 * for use by other services in the platform.
 */
@ApplicationScoped
public class GraphValidationService {

    /** Logger for this service class. */
    private static final Logger LOG = Logger.getLogger(GraphValidationService.class);

    /** Injected cache for pipeline graph topology. */
    @Inject
    GraphCache graphCache;

    /**
     * Validates that a node exists in the active graph.
     * <p>
     * Checks the graph cache to ensure the node ID is present. If the node is not found,
     * throws a {@link NodeNotFoundException} with a helpful message listing available nodes.
     * <p>
     * Note: GraphCache is synchronous, so this wraps the synchronous call in a Uni
     * for reactive composition.
     *
     * @param nodeId The node ID to validate
     * @param accountId The account ID (currently not used, but kept for API compatibility)
     * @return A Uni that completes with the GraphNode if valid, or fails with NodeNotFoundException
     */
    public Uni<GraphNode> validateNodeExists(String nodeId, String accountId) {
        LOG.debugf("Validating node exists: nodeId=%s, accountId=%s", nodeId, accountId);

        // First try to get the node directly from cache
        // This works whether graph was loaded via loadGraph() or nodes added via putNode()
        return graphCache.getNode(nodeId)
            .flatMap(nodeOpt -> {
                if (nodeOpt.isPresent()) {
                    return Uni.createFrom().item(nodeOpt.get());
                }

                // Node not found - try to get available nodes from current graph for error message
                return graphCache.getCurrentGraph()
                    .map(currentGraph -> {
                        if (currentGraph != null) {
                            List<String> availableNodeIds = currentGraph.getNodesList().stream()
                                .map(GraphNode::getNodeId)
                                .collect(Collectors.toList());
                            throw new NodeNotFoundException(nodeId, availableNodeIds);
                        } else {
                            // No graph loaded and node not in cache
                            LOG.warnf("Node validation failed: node '%s' not found and no active graph loaded", nodeId);
                            throw new NodeNotFoundException(nodeId);
                        }
                    });
            });
    }

    /**
     * Validates that all outgoing edges from a node target valid nodes in the graph.
     * <p>
     * Checks each edge's target node ID to ensure it exists in the graph cache.
     * If any edge targets a non-existent node, throws an exception with details.
     * <p>
     * Note: GraphCache is synchronous, so this wraps the synchronous call in a Uni
     * for reactive composition.
     *
     * @param node The source node whose outgoing edges should be validated
     * @param graph The pipeline graph to validate against (can be null, will use current graph)
     * @return A Uni that completes successfully if all edge targets are valid, or fails with validation error
     */
    public Uni<Void> validateEdgeTargets(GraphNode node, PipelineGraph graph) {
        LOG.debugf("Validating edge targets for node: %s", node.getNodeId());
        
        // GraphCache is now reactive
        Uni<PipelineGraph> graphUni = graph != null 
            ? Uni.createFrom().item(graph)
            : graphCache.getCurrentGraph();
        
        return graphUni
            .flatMap(currentGraph -> {
                if (currentGraph == null) {
                    LOG.warnf("Edge validation failed: no active graph loaded");
                    return Uni.createFrom().failure(new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription("Cannot validate edges - no active graph is loaded")));
                }
                
                return graphCache.getOutgoingEdges(node.getNodeId())
                    .flatMap(outgoingEdges -> {
                        if (outgoingEdges.isEmpty()) {
                            // Terminal node, no edges to validate
                            return Uni.createFrom().voidItem();
                        }
                        
                        // Build list of target node IDs to validate
                        List<String> targetNodeIds = outgoingEdges.stream()
                            .map(GraphEdge::getToNodeId)
                            .filter(id -> id != null && !id.isEmpty())
                            .toList();
                        
                        if (targetNodeIds.isEmpty()) {
                            // All edges have empty targets - check for that
                            List<String> emptyTargets = outgoingEdges.stream()
                                .filter(edge -> edge.getToNodeId() == null || edge.getToNodeId().isEmpty())
                                .map(edge -> String.format("edge '%s' (empty target)", edge.getEdgeId()))
                                .toList();
                            if (!emptyTargets.isEmpty()) {
                                String errorMsg = String.format(
                                    "Node '%s' has edges with empty targets: %s",
                                    node.getNodeId(), String.join(", ", emptyTargets));
                                LOG.errorf(errorMsg);
                                return Uni.createFrom().failure(new StatusRuntimeException(
                                    Status.INVALID_ARGUMENT.withDescription(errorMsg)));
                            }
                            return Uni.createFrom().voidItem();
                        }
                        
                        // Validate all target nodes exist (reactive - validate in parallel)
                        List<Uni<String>> validationTasks = targetNodeIds.stream()
                            .map(targetNodeId -> graphCache.getNode(targetNodeId)
                                .map(nodeOpt -> nodeOpt.isEmpty() ? targetNodeId : null))
                            .toList();
                        
                        return Uni.join().all(validationTasks).andFailFast()
                            .map(missingNodeIds -> {
                                List<String> invalidTargets = new ArrayList<>(
                                    missingNodeIds.stream()
                                        .filter(id -> id != null)
                                        .toList());
                                
                                // Also check for edges with empty targets
                                outgoingEdges.stream()
                                    .filter(edge -> edge.getToNodeId() == null || edge.getToNodeId().isEmpty())
                                    .forEach(edge -> invalidTargets.add(
                                        String.format("edge '%s' (empty target)", edge.getEdgeId())));
                                
                                if (!invalidTargets.isEmpty()) {
                                    String errorMsg = String.format(
                                        "Node '%s' has edges targeting non-existent nodes: %s",
                                        node.getNodeId(),
                                        String.join(", ", invalidTargets));
                                    LOG.errorf(errorMsg);
                                    throw new StatusRuntimeException(
                                        Status.INVALID_ARGUMENT.withDescription(errorMsg));
                                }
                                return null; // Void return
                            })
                            .replaceWithVoid();
                    });
            });
    }

    /**
     * Validates that the module referenced by a node is registered in the graph cache.
     * <p>
     * Checks that the module definition exists in the cache. Modules are typically
     * registered separately from graph loading, so this validates module registration
     * rather than just graph structure.
     *
     * @param moduleId The module ID to validate
     * @return A Uni that completes successfully if the module is registered, or fails with UNAVAILABLE status
     */
    public Uni<Void> validateModuleRegistered(String moduleId) {
        LOG.debugf("Validating module registered: %s", moduleId);
        
        return graphCache.getModule(moduleId)
            .map(moduleOpt -> {
                if (moduleOpt.isEmpty()) {
                    String errorMsg = String.format(
                        "Module '%s' is not registered in the graph cache. " +
                        "Ensure the module definition has been loaded before processing.",
                        moduleId);
                    LOG.warnf(errorMsg);
                    throw new StatusRuntimeException(
                        Status.UNAVAILABLE.withDescription(errorMsg));
                }
                return null; // Void return
            })
            .replaceWithVoid();
    }
}

