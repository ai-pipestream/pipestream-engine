package ai.pipestream.engine.validation;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Exception thrown when a node ID is not found in the active graph.
 * <p>
 * This exception provides a clear error message including the missing node ID
 * and a list of available nodes to help with debugging configuration errors.
 * It maps to gRPC NOT_FOUND status for proper error propagation.
 */
public class NodeNotFoundException extends StatusRuntimeException {
    
    private final String nodeId;
    private final List<String> availableNodeIds;
    
    /**
     * Creates a NodeNotFoundException with the missing node ID and list of available nodes.
     *
     * @param nodeId The node ID that was not found
     * @param availableNodeIds List of node IDs that are available in the graph
     */
    public NodeNotFoundException(String nodeId, List<String> availableNodeIds) {
        super(Status.NOT_FOUND.withDescription(buildMessage(nodeId, availableNodeIds)));
        this.nodeId = nodeId;
        this.availableNodeIds = availableNodeIds;
    }
    
    /**
     * Creates a NodeNotFoundException when no graph is loaded.
     *
     * @param nodeId The node ID that was requested
     */
    public NodeNotFoundException(String nodeId) {
        super(Status.NOT_FOUND.withDescription(
            String.format("Node '%s' not found - no active graph is loaded", nodeId)));
        this.nodeId = nodeId;
        this.availableNodeIds = List.of();
    }
    
    /**
     * Builds a helpful error message with the missing node ID and available nodes.
     */
    private static String buildMessage(String nodeId, List<String> availableNodeIds) {
        if (availableNodeIds.isEmpty()) {
            return String.format("Node '%s' not found in active graph. No nodes are currently available.", nodeId);
        }
        
        String availableList = availableNodeIds.stream()
            .sorted()
            .collect(Collectors.joining(", ", "[", "]"));
        
        return String.format("Node '%s' not found in active graph. Available nodes: %s", 
            nodeId, availableList);
    }
    
    /**
     * Returns the node ID that was not found.
     */
    public String getNodeId() {
        return nodeId;
    }
    
    /**
     * Returns the list of available node IDs in the graph (empty if no graph is loaded).
     */
    public List<String> getAvailableNodeIds() {
        return availableNodeIds;
    }
}

