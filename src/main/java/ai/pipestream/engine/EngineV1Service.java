package ai.pipestream.engine;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.config.v1.*;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StepExecutionRecord;
import ai.pipestream.engine.graph.GraphCache;
import ai.pipestream.engine.hydration.RepoClient;
import ai.pipestream.engine.routing.CelEvaluatorService;
import ai.pipestream.engine.v1.*;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Implementation of the Pipestream Engine V1 gRPC service.
 * <p>
 * This service orchestrates document processing through pipeline graphs, handling:
 * - Document intake from Kafka sidecar via {@link #intakeHandoff(IntakeHandoffRequest)}
 * - Node-by-node processing via {@link #processNode(ProcessNodeRequest)}
 * - Cross-cluster routing via {@link #routeToCluster(RouteToClusterRequest)}
 * - Streaming processing via {@link #processStream(Multi)}
 * - Health monitoring via {@link #getHealth(GetHealthRequest)}
 * - Kafka topic subscription management
 * <p>
 * Uses reactive Mutiny APIs throughout for non-blocking operation.
 */
@GrpcService
public class EngineV1Service extends MutinyEngineV1ServiceGrpc.EngineV1ServiceImplBase {

    /** Logger for this service class. */
    private static final Logger LOG = Logger.getLogger(EngineV1Service.class);

    /** Injected cache for pipeline graph topology and node definitions. */
    @Inject
    GraphCache graphCache;

    /** Injected client for interacting with the Repository Service for hydration/dehydration. */
    @Inject
    RepoClient repoClient;

    /** Injected service for evaluating CEL (Common Expression Language) conditions for routing. */
    @Inject
    CelEvaluatorService celEvaluator;

    /** Injected factory for creating dynamic gRPC clients for module communication. */
    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    /** Injected emitter for sending PipeStream messages to Kafka routing topics. */
    @Inject
    @ProtobufChannel("engine-routing-out")
    ProtobufEmitter<PipeStream> routingEmitter;

    /** Configuration property defining the current cluster ID for routing decisions. */
    @ConfigProperty(name = "pipestream.cluster.id", defaultValue = "default-cluster")
    String currentClusterId;

    /**
     * Processes a single node in the pipeline graph. Inherited from {@link MutinyEngineV1ServiceGrpc.EngineV1ServiceImplBase}.
     * <p>
     * This is the core orchestration method that:
     * 1. Validates the requested node exists in the graph
     * 2. Executes the node processing logic (hydration, module call, routing)
     * 3. Returns success/failure response with updated stream
     *
     * @param request The processing request containing the stream and target node
     * @return A Uni that completes with the processing response
     */
    @Override
    public Uni<ProcessNodeResponse> processNode(ProcessNodeRequest request) {
        PipeStream stream = request.getStream();
        String nodeId = stream.getCurrentNodeId();
        
        LOG.debugf("ProcessNode: Stream %s at Node %s", stream.getStreamId(), nodeId);

        return Uni.createFrom().item(() -> graphCache.getNode(nodeId))
            .flatMap(nodeOpt -> {
                if (nodeOpt.isEmpty()) {
                    return Uni.createFrom().failure(new RuntimeException("Node not found in graph: " + nodeId));
                }
                return Uni.createFrom().item(nodeOpt.get());
            })
            .flatMap(node -> processNodeLogic(stream, node))
            .map(updatedStream -> ProcessNodeResponse.newBuilder()
                    .setSuccess(true)
                    .setUpdatedStream(updatedStream)
                    .build())
            .onFailure().recoverWithItem(t -> {
                LOG.errorf("Error processing node %s: %s", nodeId, t.getMessage());
                return ProcessNodeResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage(t.getMessage())
                        .build();
            });
    }

    /**
     * Internal method that executes the complete node processing pipeline.
     * <p>
     * The processing follows this sequence:
     * 1. Ensure document is hydrated to required level
     * 2. Call the configured module for processing
     * 3. Update stream metadata and execution history
     * 4. Route the result to next nodes in the graph
     *
     * @param stream The input stream to process
     * @param node The graph node configuration defining the processing logic
     * @return A Uni that completes with the processed stream
     */
    private Uni<PipeStream> processNodeLogic(PipeStream stream, GraphNode node) {
        // 3. Hydrate (Level 2) if needed
        return ensureHydration(stream, node)
            .flatMap(hydratedStream -> callModule(hydratedStream, node))
            .flatMap(resultDoc -> {
                // 5. Update Metadata & History
                PipeStream updatedStream = updateStreamMetadata(stream, resultDoc, node);
                
                // 6. Route to Next
                return routeToNextNodes(updatedStream, node);
            });
    }

    /**
     * Ensures the document in the stream is hydrated to the level required by the node.
     * <p>
     * Currently simplified - assumes Level 2 hydration is not needed.
     * In future implementations, this will check node capabilities and fetch
     * additional document data from the repository as needed.
     *
     * @param stream The stream containing the document to potentially hydrate
     * @param node The node configuration specifying hydration requirements
     * @return A Uni that completes with the hydrated stream
     */
    private Uni<PipeStream> ensureHydration(PipeStream stream, GraphNode node) {
        // Note: Logic for checking capabilities suppressed for now due to compilation issues
        // with missing field in generated code. We assume no Level 2 hydration for now.
        return Uni.createFrom().item(stream);
    }

    /**
     * Calls the configured module for this node via dynamic gRPC.
     * <p>
     * Uses the DynamicGrpcClientFactory to create a client for the module's
     * gRPC service. The service name is resolved from the module definition
     * in the graph cache, falling back to the module ID if not specified.
     *
     * @param stream The input stream with document to process
     * @param node The node configuration specifying which module to call
     * @return A Uni that completes with the processed document output
     * @throws RuntimeException if module processing fails or returns error
     */
    private Uni<PipeDoc> callModule(PipeStream stream, GraphNode node) {
        String moduleId = node.getModuleId();
        String serviceName = graphCache.getModule(moduleId)
                .map(ModuleDefinition::getGrpcServiceName)
                .filter(s -> !s.isEmpty())
                .orElse(moduleId);

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(stream.getDocument())
                .setMetadata(ServiceMetadata.newBuilder()
                        .setStreamId(stream.getStreamId())
                        .setCurrentHopNumber(stream.getHopCount())
                        .build())
                .build();

        return grpcClientFactory.getClient(serviceName, MutinyPipeStepProcessorServiceGrpc::newMutinyStub)
                .flatMap(stub -> stub.processData(request))
                .map(response -> {
                    if (!response.getSuccess()) {
                        String errorMsg = "Module processing failed";
                        if (response.hasErrorDetails()) {
                            errorMsg += ": " + response.getErrorDetails().toString();
                        }
                        throw new RuntimeException(errorMsg);
                    }
                    return response.getOutputDoc();
                });
    }

    /**
     * Updates the stream metadata after successful module processing.
     * <p>
     * Creates execution history records, increments hop count, updates the
     * current node ID, and records the processing timestamp. This metadata
     * is used for observability, debugging, and routing decisions.
     *
     * @param stream The original input stream
     * @param outputDoc The processed document output from the module
     * @param node The node that performed the processing
     * @return A new PipeStream instance with updated metadata
     */
    private PipeStream updateStreamMetadata(PipeStream stream, PipeDoc outputDoc, GraphNode node) {
        Timestamp now = Timestamps.fromMillis(Instant.now().toEpochMilli());
        
        StepExecutionRecord history = StepExecutionRecord.newBuilder()
                .setStepName(node.getName())
                .setHopNumber(stream.getHopCount() + 1)
                .setEndTime(now)
                .setStatus("SUCCESS")
                .build();

        return stream.toBuilder()
                .setDocument(outputDoc)
                .setHopCount(stream.getHopCount() + 1)
                .setCurrentNodeId(node.getNodeId())
                .addProcessingPath(node.getNodeId())
                .setMetadata(stream.getMetadata().toBuilder()
                        .setLastProcessedAt(now)
                        .addHistory(history)
                        .build())
                .build();
    }

    /**
     * Routes the processed stream to all matching downstream nodes.
     * <p>
     * Evaluates edge conditions using CEL and dispatches the stream to all
     * edges where conditions match. If no edges match, the stream terminates.
     * Multiple edges can match, allowing for fan-out routing patterns.
     *
     * @param stream The processed stream to route
     * @param node The source node (used to find outgoing edges)
     * @return A Uni that completes when all routing operations finish
     */
    private Uni<PipeStream> routeToNextNodes(PipeStream stream, GraphNode node) {
        List<GraphEdge> edges = graphCache.getOutgoingEdges(node.getNodeId());
        
        List<Uni<Void>> routings = edges.stream()
            .filter(edge -> celEvaluator.evaluate(edge.getCondition(), stream))
            .map(edge -> dispatch(stream, edge))
            .collect(Collectors.toList());
            
        if (routings.isEmpty()) {
            LOG.debugf("Stream %s finished at node %s (terminal)", stream.getStreamId(), node.getNodeId());
            return Uni.createFrom().item(stream);
        }

        return Uni.combine().all().unis(routings).discardItems()
                .map(v -> stream);
    }

    /**
     * Dispatches a stream to a target node via the specified transport mechanism.
     * <p>
     * Supports two transport types:
     * - MESSAGING: Saves document to repository, creates reference, sends to Kafka topic
     * - LOCAL: Direct gRPC call to next node (same cluster only)
     * <p>
     * For messaging transport, ensures deterministic partitioning using UUID keys
     * derived from the stream ID for proper message ordering.
     *
     * @param stream The stream to dispatch
     * @param edge The edge configuration specifying transport and destination
     * @return A Uni that completes when dispatch is successful
     */
    private Uni<Void> dispatch(PipeStream stream, GraphEdge edge) {
        PipeStream nextStream = stream.toBuilder()
                .setCurrentNodeId(edge.getToNodeId())
                .build();

        if (edge.getTransportType() == TransportType.TRANSPORT_TYPE_MESSAGING) {
            return repoClient.savePipeDoc(nextStream.getDocument(), "default")
                .flatMap(nodeId -> {
                    PipeDoc refDoc = PipeDoc.newBuilder()
                            .setDocId(nextStream.getDocument().getDocId())
                            .setOwnership(nextStream.getDocument().getOwnership())
                            .build();
                            
                    PipeStream refStream = nextStream.toBuilder()
                            .setDocument(refDoc)
                            .build();

                    String topic = edge.getKafkaTopic();
                    if (topic == null || topic.isEmpty()) {
                        String targetCluster = edge.getToClusterId();
                        if (targetCluster == null || targetCluster.isEmpty()) {
                            targetCluster = currentClusterId;
                        }
                        topic = String.format("pipestream.%s.%s", targetCluster, edge.getToNodeId());
                    }

                    LOG.debugf("Routing to Kafka topic: %s", topic);
                    
                    // Extract UUID key for partitioning (ensures same stream goes to same partition)
                    UUID key;
                    try {
                        key = UUID.fromString(refStream.getStreamId());
                    } catch (IllegalArgumentException e) {
                        // If not a valid UUID, create deterministic UUID from stream_id
                        key = UUID.nameUUIDFromBytes(refStream.getStreamId().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    }
                    
                    // For dynamic topics, we need to add topic metadata
                    final String finalTopic = topic;
                    OutgoingKafkaRecordMetadata<UUID> metadata = OutgoingKafkaRecordMetadata.<UUID>builder()
                            .withKey(key)
                            .withTopic(finalTopic)
                            .build();
                    
                    // ProtobufEmitter.send(Message) is void, so we wrap in Uni for reactive composition
                    routingEmitter.send(org.eclipse.microprofile.reactive.messaging.Message.of(refStream)
                            .addMetadata(metadata));
                    
                    return Uni.createFrom().voidItem();
                });

        } else {
            if (edge.getIsCrossCluster()) {
                return Uni.createFrom().voidItem();
            } else {
                return processNode(ProcessNodeRequest.newBuilder().setStream(nextStream).build())
                        .replaceWithVoid();
            }
        }
    }

    /**
     * Handles document intake from the Kafka sidecar. Inherited from {@link MutinyEngineV1ServiceGrpc.EngineV1ServiceImplBase}.
     * <p>
     * This is the primary entry point for documents entering the pipeline.
     * Creates a new stream ID, finds the appropriate entry node for the datasource,
     * and initiates processing from that entry point.
     *
     * @param request The intake request containing the document and datasource identifier
     * @return A Uni that completes with acceptance status and any error messages
     */
    @Override
    public Uni<IntakeHandoffResponse> intakeHandoff(IntakeHandoffRequest request) {
        return Uni.createFrom().item(() -> graphCache.getEntryNodeId(request.getDatasourceId()))
            .flatMap(nodeIdOpt -> {
                if (nodeIdOpt.isEmpty()) {
                     return Uni.createFrom().failure(new RuntimeException("No entry node for datasource: " + request.getDatasourceId()));
                }
                String nodeId = nodeIdOpt.get();
                
                PipeStream stream = request.getStream().toBuilder()
                        .setStreamId(UUID.randomUUID().toString())
                        .setCurrentNodeId(nodeId)
                        .setHopCount(0)
                        .build();

                return processNode(ProcessNodeRequest.newBuilder().setStream(stream).build());
            })
            .map(resp -> IntakeHandoffResponse.newBuilder()
                    .setAccepted(resp.getSuccess())
                    .setMessage(resp.getMessage())
                    .build());
    }

    /**
     * Handles cross-cluster routing requests. Inherited from {@link MutinyEngineV1ServiceGrpc.EngineV1ServiceImplBase}.
     * <p>
     * Currently a stub implementation. Future versions will implement
     * cross-cluster document routing via Kafka topics or direct gRPC calls.
     *
     * @param request The routing request specifying destination cluster and document
     * @return A Uni that completes with routing success status
     */
    @Override
    public Uni<RouteToClusterResponse> routeToCluster(RouteToClusterRequest request) {
         return Uni.createFrom().item(RouteToClusterResponse.newBuilder().setSuccess(true).build());
    }

    /**
     * Handles streaming processing requests. Inherited from {@link MutinyEngineV1ServiceGrpc.EngineV1ServiceImplBase}.
     * <p>
     * Currently a stub implementation that acknowledges each request.
     * Future versions will implement real-time streaming processing
     * for continuous document flows.
     *
     * @param request A Multi stream of processing requests
     * @return A Multi stream of processing responses
     */
    @Override
    public Multi<ProcessStreamResponse> processStream(Multi<ProcessStreamRequest> request) {
        return request.map(r -> ProcessStreamResponse.newBuilder().setSuccess(true).build());
    }

    /**
     * Provides health status of the engine service. Inherited from {@link MutinyEngineV1ServiceGrpc.EngineV1ServiceImplBase}.
     * <p>
     * Currently returns a static healthy status. Future implementations
     * may include checks for dependencies (Consul, Kafka, Repository service).
     *
     * @param request The health check request
     * @return A Uni that completes with the current health status
     */
    @Override
    public Uni<GetHealthResponse> getHealth(GetHealthRequest request) {
         return Uni.createFrom().item(GetHealthResponse.newBuilder().setHealth(EngineHealth.ENGINE_HEALTH_HEALTHY).build());
    }

    /**
     * Updates Kafka topic subscriptions for dynamic routing. Inherited from {@link MutinyEngineV1ServiceGrpc.EngineV1ServiceImplBase}.
     * <p>
     * Currently a stub implementation. Future versions will dynamically
     * subscribe/unsubscribe to Kafka topics based on graph topology changes.
     *
     * @param request The subscription update request
     * @return A Uni that completes with update success status
     */
    @Override
    public Uni<UpdateTopicSubscriptionsResponse> updateTopicSubscriptions(UpdateTopicSubscriptionsRequest request) {
         return Uni.createFrom().item(UpdateTopicSubscriptionsResponse.newBuilder().setSuccess(true).build());
    }

    /**
     * Retrieves current Kafka topic subscriptions. Inherited from {@link MutinyEngineV1ServiceGrpc.EngineV1ServiceImplBase}.
     * <p>
     * Currently returns an empty response. Future versions will return
     * the list of topics the engine is currently subscribed to.
     *
     * @param request The subscription query request
     * @return A Uni that completes with current subscription information
     */
    @Override
    public Uni<GetTopicSubscriptionsResponse> getTopicSubscriptions(GetTopicSubscriptionsRequest request) {
         return Uni.createFrom().item(GetTopicSubscriptionsResponse.newBuilder().build());
    }
}