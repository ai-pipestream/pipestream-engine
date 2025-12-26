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

@GrpcService
public class EngineV1Service extends MutinyEngineV1ServiceGrpc.EngineV1ServiceImplBase {

    private static final Logger LOG = Logger.getLogger(EngineV1Service.class);

    @Inject
    GraphCache graphCache;

    @Inject
    RepoClient repoClient;

    @Inject
    CelEvaluatorService celEvaluator;

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    @Inject
    @ProtobufChannel("engine-routing-out")
    ProtobufEmitter<PipeStream> routingEmitter;
    
    @ConfigProperty(name = "pipestream.cluster.id", defaultValue = "default-cluster")
    String currentClusterId;

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

    private Uni<PipeStream> ensureHydration(PipeStream stream, GraphNode node) {
        // Note: Logic for checking capabilities suppressed for now due to compilation issues
        // with missing field in generated code. We assume no Level 2 hydration for now.
        return Uni.createFrom().item(stream);
    }

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

    @Override
    public Uni<RouteToClusterResponse> routeToCluster(RouteToClusterRequest request) {
         return Uni.createFrom().item(RouteToClusterResponse.newBuilder().setSuccess(true).build());
    }

    @Override
    public Multi<ProcessStreamResponse> processStream(Multi<ProcessStreamRequest> request) {
        return request.map(r -> ProcessStreamResponse.newBuilder().setSuccess(true).build());
    }

    @Override
    public Uni<GetHealthResponse> getHealth(GetHealthRequest request) {
         return Uni.createFrom().item(GetHealthResponse.newBuilder().setHealth(EngineHealth.ENGINE_HEALTH_HEALTHY).build());
    }

    @Override
    public Uni<UpdateTopicSubscriptionsResponse> updateTopicSubscriptions(UpdateTopicSubscriptionsRequest request) {
         return Uni.createFrom().item(UpdateTopicSubscriptionsResponse.newBuilder().setSuccess(true).build());
    }

    @Override
    public Uni<GetTopicSubscriptionsResponse> getTopicSubscriptions(GetTopicSubscriptionsRequest request) {
         return Uni.createFrom().item(GetTopicSubscriptionsResponse.newBuilder().build());
    }
}