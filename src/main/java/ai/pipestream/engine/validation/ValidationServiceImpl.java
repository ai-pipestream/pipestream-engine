package ai.pipestream.engine.validation;

import ai.pipestream.config.v1.GraphNode;
import ai.pipestream.validation.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * gRPC service implementation for the ValidationService.
 * <p>
 * Exposes graph validation functionality to other services in the platform.
 * Delegates actual validation logic to {@link GraphValidationService}.
 * <p>
 * This service handles:
 * - Validating that nodes exist in the active graph (for runtime validation)
 * - Validating graph structure and edge targets
 * - Validating module registration
 */
@GrpcService
public class ValidationServiceImpl extends MutinyValidationServiceGrpc.ValidationServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ValidationServiceImpl.class);

    private final GraphValidationService graphValidationService;

    /**
     * CDI constructor.
     */
    @Inject
    public ValidationServiceImpl(GraphValidationService graphValidationService) {
        this.graphValidationService = graphValidationService;
    }

    /**
     * Validates a graph node configuration.
     * <p>
     * For runtime validation, checks if the node exists in the active graph.
     * The request contains the node configuration wrapped in Any.
     */
    @Override
    public Uni<ValidateNodeResponse> validateNode(ValidateNodeRequest request) {
        LOG.debugf("ValidateNode request: mode=%s, cluster_id=%s", request.getMode(), request.getClusterId());

        try {
            // Unpack the node from Any
            if (!request.hasNode()) {
                return Uni.createFrom().item(ValidateNodeResponse.newBuilder()
                    .setIsValid(false)
                    .addErrors(ValidationError.newBuilder()
                        .setErrorType(ValidationErrorType.VALIDATION_ERROR_TYPE_REQUIRED_FIELD)
                        .setFieldPath("node")
                        .setMessage("Node configuration is required")
                        .build())
                    .build());
            }

            Any nodeAny = request.getNode();
            GraphNode node = nodeAny.unpack(GraphNode.class);

            // Extract account ID from cluster ID (for now, using cluster_id as account context)
            String accountId = request.getClusterId();

            // Validate node exists in active graph
            return graphValidationService.validateNodeExists(node.getNodeId(), accountId)
                .map(validatedNode -> {
                    // Node exists - validation successful
                    return ValidateNodeResponse.newBuilder()
                        .setIsValid(true)
                        .build();
                })
                .onFailure(NodeNotFoundException.class).recoverWithItem(e -> {
                    NodeNotFoundException nfe = (NodeNotFoundException) e;
                    // Node not found - create validation error response
                    ValidationError.Builder errorBuilder = ValidationError.newBuilder()
                        .setErrorType(ValidationErrorType.VALIDATION_ERROR_TYPE_MODULE_NOT_FOUND)
                        .setFieldPath("node.node_id")
                        .setMessage(nfe.getMessage())
                        .setErrorCode("NODE_NOT_FOUND");

                    // Add available nodes as context if available
                    if (!nfe.getAvailableNodeIds().isEmpty()) {
                        errorBuilder.setMessage(nfe.getMessage() + 
                            " Available nodes: " + String.join(", ", nfe.getAvailableNodeIds()));
                    }

                    return ValidateNodeResponse.newBuilder()
                        .setIsValid(false)
                        .addErrors(errorBuilder.build())
                        .build();
                })
                .onFailure().recoverWithItem(throwable -> {
                    LOG.errorf(throwable, "Error validating node: %s", node.getNodeId());
                    return ValidateNodeResponse.newBuilder()
                        .setIsValid(false)
                        .addErrors(ValidationError.newBuilder()
                            .setErrorType(ValidationErrorType.VALIDATION_ERROR_TYPE_CUSTOM)
                            .setFieldPath("node")
                            .setMessage("Validation failed: " + throwable.getMessage())
                            .setErrorCode("VALIDATION_ERROR")
                            .build())
                        .build();
                });
        } catch (InvalidProtocolBufferException e) {
            LOG.errorf(e, "Failed to unpack node from Any");
            return Uni.createFrom().item(ValidateNodeResponse.newBuilder()
                .setIsValid(false)
                .addErrors(ValidationError.newBuilder()
                    .setErrorType(ValidationErrorType.VALIDATION_ERROR_TYPE_INVALID_FORMAT)
                    .setFieldPath("node")
                    .setMessage("Invalid node configuration format: " + e.getMessage())
                    .setErrorCode("INVALID_FORMAT")
                    .build())
                .build());
        }
    }

    /**
     * Validates a complete pipeline graph for correctness.
     * <p>
     * Currently returns a stub response. Full graph validation would validate
     * all nodes exist, all edges target valid nodes, and graph structure is correct.
     */
    @Override
    public Uni<ValidateGraphResponse> validateGraph(ValidateGraphRequest request) {
        LOG.debugf("ValidateGraph request: mode=%s, cluster_id=%s", request.getMode(), request.getClusterId());
        
        // TODO: Implement full graph validation
        // This would validate:
        // - All nodes exist and are valid
        // - All edges target valid nodes
        // - No circular dependencies
        // - All modules are registered
        // - Graph structure is consistent
        
        return Uni.createFrom().item(ValidateGraphResponse.newBuilder()
            .setIsValid(true)
            .build());
    }

    /**
     * Validates a module configuration and connectivity.
     * <p>
     * Validates that the module is registered in the graph cache.
     */
    @Override
    public Uni<ValidateModuleResponse> validateModule(ValidateModuleRequest request) {
        LOG.debugf("ValidateModule request: mode=%s", request.getMode());

        try {
            // Unpack the module from Any
            if (!request.hasModule()) {
                return Uni.createFrom().item(ValidateModuleResponse.newBuilder()
                    .setIsValid(false)
                    .addErrors(ValidationError.newBuilder()
                        .setErrorType(ValidationErrorType.VALIDATION_ERROR_TYPE_REQUIRED_FIELD)
                        .setFieldPath("module")
                        .setMessage("Module configuration is required")
                        .build())
                    .build());
            }

            // TODO: Implement module unpacking and validation
            // Note: ModuleDefinition would need to be unpacked from Any
            // For now, return stub response
            return Uni.createFrom().item(ValidateModuleResponse.newBuilder()
                .setIsValid(true)
                .build());
        } catch (Exception e) {
            LOG.errorf(e, "Error validating module");
            return Uni.createFrom().item(ValidateModuleResponse.newBuilder()
                .setIsValid(false)
                .addErrors(ValidationError.newBuilder()
                    .setErrorType(ValidationErrorType.VALIDATION_ERROR_TYPE_CUSTOM)
                    .setFieldPath("module")
                    .setMessage("Validation failed: " + e.getMessage())
                    .setErrorCode("VALIDATION_ERROR")
                    .build())
                .build());
        }
    }
}

