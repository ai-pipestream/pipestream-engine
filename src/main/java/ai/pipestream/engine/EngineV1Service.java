package ai.pipestream.engine;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.config.v1.*;
import ai.pipestream.data.module.v1.*;
import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.Blobs;
import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.ProcessConfiguration;
import ai.pipestream.data.v1.StepExecutionRecord;
import ai.pipestream.engine.graph.GraphCache;
import ai.pipestream.engine.hydration.RepoClient;
import ai.pipestream.engine.mapping.MappingEngine;
import ai.pipestream.engine.module.ModuleCapabilityService;
import ai.pipestream.engine.routing.CelEvaluatorService;
import ai.pipestream.engine.validation.GraphValidationService;
import ai.pipestream.engine.v1.*;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

    /** Injected service for querying module capabilities to determine hydration requirements. */
    @Inject
    ModuleCapabilityService capabilityService;

    /** Injected engine for applying field mappings before/after module processing. */
    @Inject
    MappingEngine mappingEngine;

    /** Injected service for validating graph structure and node references. */
    @Inject
    GraphValidationService graphValidationService;

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

    /** Maximum number of retry attempts for module calls (default: 3). */
    @ConfigProperty(name = "pipestream.module.retry.max-attempts", defaultValue = "3")
    int moduleRetryMaxAttempts;

    /** Initial delay for exponential backoff in milliseconds (default: 100ms). */
    @ConfigProperty(name = "pipestream.module.retry.initial-delay-ms", defaultValue = "100")
    long moduleRetryInitialDelayMs;

    /** Maximum delay for exponential backoff in milliseconds (default: 2000ms). */
    @ConfigProperty(name = "pipestream.module.retry.max-delay-ms", defaultValue = "2000")
    long moduleRetryMaxDelayMs;

    /** Service instance identifier (pod name/IP) for execution metadata tracking. */
    @ConfigProperty(name = "pipestream.engine.instance-id", defaultValue = "${HOSTNAME:unknown}")
    String serviceInstanceId;

    /** gRPC status codes that trigger retry (UNAVAILABLE, DEADLINE_EXCEEDED by default). */
    private static final Set<Status.Code> RETRYABLE_STATUS_CODES = Set.of(
            Status.Code.UNAVAILABLE,
            Status.Code.DEADLINE_EXCEEDED,
            Status.Code.RESOURCE_EXHAUSTED
    );

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

        // Extract accountId from stream metadata (default to empty string if not available)
        String accountId = stream.hasMetadata() 
            ? stream.getMetadata().getAccountId() 
            : "";

        // Validate node exists in graph before processing
        return graphValidationService.validateNodeExists(nodeId, accountId)
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
     * 2. Evaluate filter conditions - skip if any evaluates to false
     * 3. Apply pre-mappings to transform document before module call
     * 4. Call the configured module for processing
     * 5. Apply post-mappings to transform document after module call
     * 6. Update stream metadata and execution history
     * 7. Route the result to next nodes in the graph
     *
     * @param stream The input stream to process
     * @param node The graph node configuration defining the processing logic
     * @return A Uni that completes with the processed stream
     */
    private Uni<PipeStream> processNodeLogic(PipeStream stream, GraphNode node) {
        // Capture start time for execution metadata
        final Instant startTime = Instant.now();

        // 1. Hydrate (Level 1 & 2) if needed
        return ensureHydration(stream, node)
            .flatMap(hydratedStream -> {
                // 2. Evaluate filter conditions - all must pass
                if (!evaluateFilterConditions(hydratedStream, node)) {
                    LOG.debugf("Stream %s skipped at node %s - filter condition not met",
                            hydratedStream.getStreamId(), node.getNodeId());
                    // Skip this node, route directly to next nodes without processing
                    return routeToNextNodes(hydratedStream, node);
                }

                // 3. Apply pre-mappings
                return applyPreMappings(hydratedStream, node)
                    // 4. Call module
                    .flatMap(preMappedStream -> callModule(preMappedStream, node)
                        // 5. Apply post-mappings
                        .flatMap(resultDoc -> applyPostMappings(resultDoc, preMappedStream, node)))
                    .flatMap(postMappedDoc -> {
                        // 6. Update Metadata & History (with start time for duration calculation)
                        PipeStream updatedStream = updateStreamMetadata(hydratedStream, postMappedDoc, node, startTime);

                        // 7. Route to Next
                        return routeToNextNodes(updatedStream, node);
                    });
            })
            .onFailure().recoverWithUni(error -> {
                // Handle save_on_error behavior
                if (node.getSaveOnError() && stream.hasDocument()) {
                    LOG.warnf("Module %s failed, saving document due to save_on_error=true: %s",
                            node.getModuleId(), error.getMessage());
                    return saveErrorState(stream, node, error)
                            .flatMap(v -> Uni.createFrom().failure(error));
                }
                return Uni.createFrom().failure(error);
            });
    }

    /**
     * Evaluates all filter conditions for a node.
     * <p>
     * All conditions must evaluate to true for the document to be processed.
     * If any condition evaluates to false, the document skips this node.
     * An empty filter list means no filtering (document is processed).
     *
     * @param stream The stream to evaluate conditions against
     * @param node The node containing filter conditions
     * @return true if all conditions pass (or no conditions), false otherwise
     */
    private boolean evaluateFilterConditions(PipeStream stream, GraphNode node) {
        if (node.getFilterConditionsCount() == 0) {
            return true; // No filters, process the document
        }

        for (String condition : node.getFilterConditionsList()) {
            if (condition == null || condition.isBlank()) {
                continue; // Skip empty conditions
            }
            if (!celEvaluator.evaluate(condition, stream)) {
                LOG.debugf("Filter condition failed: %s", condition);
                return false;
            }
        }
        return true; // All conditions passed
    }

    /**
     * Applies pre-mappings to transform the document before module processing.
     *
     * @param stream The input stream
     * @param node The node containing pre-mapping configuration
     * @return A Uni with the stream containing the transformed document
     */
    private Uni<PipeStream> applyPreMappings(PipeStream stream, GraphNode node) {
        if (node.getPreMappingsCount() == 0 || !stream.hasDocument()) {
            return Uni.createFrom().item(stream);
        }

        return mappingEngine.applyMappings(stream.getDocument(), node.getPreMappingsList(), stream)
                .map(mappedDoc -> stream.toBuilder().setDocument(mappedDoc).build());
    }

    /**
     * Applies post-mappings to transform the document after module processing.
     *
     * @param resultDoc The document returned from the module
     * @param stream The original stream (for CEL context)
     * @param node The node containing post-mapping configuration
     * @return A Uni with the transformed document
     */
    private Uni<PipeDoc> applyPostMappings(PipeDoc resultDoc, PipeStream stream, GraphNode node) {
        if (node.getPostMappingsCount() == 0) {
            return Uni.createFrom().item(resultDoc);
        }

        // Create updated stream context with the result document for CEL evaluation
        PipeStream updatedContext = stream.toBuilder().setDocument(resultDoc).build();
        return mappingEngine.applyMappings(resultDoc, node.getPostMappingsList(), updatedContext);
    }

    /**
     * Saves document state when processing fails and save_on_error is enabled.
     *
     * @param stream The stream containing the document to save
     * @param node The node where processing failed
     * @param error The error that occurred
     * @return A Uni that completes when the document is saved
     */
    private Uni<Void> saveErrorState(PipeStream stream, GraphNode node, Throwable error) {
        if (!stream.hasDocument()) {
            return Uni.createFrom().voidItem();
        }

        String accountId = stream.getMetadata().getAccountId();
        String graphLocationId = node.getNodeId() + ".error";

        return repoClient.savePipeDoc(stream.getDocument(), accountId, graphLocationId, currentClusterId)
                .replaceWithVoid()
                .onFailure().invoke(saveError ->
                        LOG.errorf(saveError, "Failed to save error state for stream %s at node %s",
                                stream.getStreamId(), node.getNodeId()));
    }

    /**
     * Ensures the document in the stream is hydrated to the level required by the node.
     * <p>
     * Level 1 Hydration: If the stream contains a DocumentReference, fetches the full
     * PipeDoc from the repository service.
     * <p>
     * Level 2 Hydration: Checks module capabilities to determine if blob content is needed.
     * If the module has {@code CAPABILITY_TYPE_PARSER}, blobs are hydrated. Otherwise,
     * blobs are left as storage references to avoid unnecessary data transfer.
     * <p>
     * The hydration decision follows this logic:
     * <ul>
     *   <li>If module has PARSER capability → hydrate blobs (parsers need raw binary)</li>
     *   <li>If module has no PARSER capability → skip blob hydration (work with parsed metadata)</li>
     *   <li>If capability query fails → skip blob hydration (safe default)</li>
     * </ul>
     *
     * @param stream The stream containing the document to potentially hydrate
     * @param node The node configuration specifying hydration requirements
     * @return A Uni that completes with the hydrated stream
     */
    private Uni<PipeStream> ensureHydration(PipeStream stream, GraphNode node) {
        String moduleId = node.getModuleId();
        
        // Level 1 Hydration: Check if we need to fetch from repository
        if (stream.hasDocumentRef()) {
            DocumentReference ref = stream.getDocumentRef();
            return repoClient.getPipeDocByReference(ref)
                    .flatMap(doc -> {
                        // After Level 1, check for Level 2 hydration based on module capabilities
                        return capabilityService.requiresBlobContent(moduleId)
                                .flatMap(needsBlob -> {
                                    if (needsBlob) {
                                        return hydrateBlobsIfNeeded(doc);
                                    } else {
                                        LOG.debugf("Module %s does not require blob content - skipping Level 2 hydration", moduleId);
                                        return Uni.createFrom().item(doc);
                                    }
                                })
                                .map(hydratedDoc -> stream.toBuilder()
                                        .clearDocumentRef()
                                        .setDocument(hydratedDoc)
                                        .build());
                    });
        }
        
        // Document is already inline, check for Level 2 hydration based on module capabilities
        if (stream.hasDocument()) {
            return capabilityService.requiresBlobContent(moduleId)
                    .flatMap(needsBlob -> {
                        if (needsBlob) {
                            return hydrateBlobsIfNeeded(stream.getDocument());
                        } else {
                            LOG.debugf("Module %s does not require blob content - skipping Level 2 hydration", moduleId);
                            return Uni.createFrom().item(stream.getDocument());
                        }
                    })
                    .map(hydratedDoc -> stream.toBuilder()
                            .setDocument(hydratedDoc)
                            .build());
        }
        
        // No document or reference - return as-is
        return Uni.createFrom().item(stream);
    }

    /**
     * Performs Level 2 blob hydration: fetches blob bytes from repository for any blobs
     * that have a storage_ref but no inline data.
     * <p>
     * This method handles both single-blob BlobBag (via getBlob()) and multi-blob BlobBag
     * (via getBlobs()). For each blob that needs hydration, it:
     * 1. Extracts the FileStorageReference
     * 2. Calls RepoClient.getBlob() to fetch the bytes (in parallel for multiple blobs)
     * 3. Replaces storage_ref with inline data
     * <p>
     * Single blobs are hydrated sequentially, while multiple blobs are hydrated in parallel
     * for better performance.
     *
     * @param doc The PipeDoc that may contain blobs needing hydration
     * @return A Uni that completes with the PipeDoc with all blobs hydrated
     */
    private Uni<PipeDoc> hydrateBlobsIfNeeded(PipeDoc doc) {
        if (!doc.hasBlobBag()) {
            // No blobs to hydrate
            return Uni.createFrom().item(doc);
        }

        BlobBag blobBag = doc.getBlobBag();

        // Check if single blob needs hydration
        if (blobBag.hasBlob()) {
            Blob blob = blobBag.getBlob();
            if (blob.hasStorageRef() && !blob.hasData()) {
                // Need to hydrate this blob
                LOG.debugf("Hydrating blob with storage_ref: drive=%s, object_key=%s", 
                        blob.getStorageRef().getDriveName(), blob.getStorageRef().getObjectKey());
                return repoClient.getBlob(blob.getStorageRef())
                        .map(blobData -> {
                            Blob hydratedBlob = blob.toBuilder()
                                    .setData(blobData)
                                    .clearStorageRef()
                                    .build();
                            LOG.debugf("Blob hydrated successfully: size=%d bytes", blobData.size());
                            return doc.toBuilder()
                                    .setBlobBag(blobBag.toBuilder()
                                            .setBlob(hydratedBlob)
                                            .build())
                                    .build();
                        })
                        .onFailure().invoke(throwable -> 
                                LOG.errorf(throwable, "Failed to hydrate blob: drive=%s, object_key=%s",
                                        blob.getStorageRef().getDriveName(), 
                                        blob.getStorageRef().getObjectKey()));
            }
            // Single blob already hydrated or has no storage_ref, return as-is
            return Uni.createFrom().item(doc);
        }

        // Check if multiple blobs need hydration
        if (blobBag.hasBlobs()) {
            return hydrateMultipleBlobs(doc, blobBag.getBlobs());
        }

        // No blobs or unknown blob type, return as-is
        return Uni.createFrom().item(doc);
    }

    /**
     * Hydrates multiple blobs in parallel for a Blobs collection.
     * <p>
     * For each blob that has a storage_ref but no inline data, fetches the blob bytes
     * from the repository service in parallel. Blobs that are already hydrated or don't
     * need hydration are returned as-is.
     *
     * @param doc The PipeDoc containing the multi-blob BlobBag
     * @param blobs The Blobs collection containing multiple blobs
     * @return A Uni that completes with the PipeDoc with all blobs hydrated
     */
    private Uni<PipeDoc> hydrateMultipleBlobs(PipeDoc doc, Blobs blobs) {
        List<Blob> blobList = blobs.getBlobList();
        
        // Check if any blobs need hydration
        boolean needsHydration = blobList.stream()
                .anyMatch(blob -> blob.hasStorageRef() && !blob.hasData());
        
        if (!needsHydration) {
            // No blobs need hydration, return as-is
            return Uni.createFrom().item(doc);
        }
        
        // Create a list of Unis, one for each blob
        // Each Uni will resolve to a hydrated Blob (or the original if already hydrated)
        List<Uni<Blob>> blobHydrationTasks = new ArrayList<>();
        
        for (int i = 0; i < blobList.size(); i++) {
            Blob blob = blobList.get(i);
            int blobIndex = i + 1; // 1-based index for logging
            
            if (blob.hasStorageRef() && !blob.hasData()) {
                // Need to hydrate this blob
                LOG.debugf("Hydrating blob %d/%d with storage_ref: drive=%s, object_key=%s",
                        blobIndex, blobList.size(),
                        blob.getStorageRef().getDriveName(), blob.getStorageRef().getObjectKey());
                
                Uni<Blob> hydratedBlobUni = repoClient.getBlob(blob.getStorageRef())
                        .map(blobData -> {
                            Blob hydratedBlob = blob.toBuilder()
                                    .setData(blobData)
                                    .clearStorageRef()
                                    .build();
                            LOG.debugf("Blob %d/%d hydrated successfully: size=%d bytes",
                                    blobIndex, blobList.size(), blobData.size());
                            return hydratedBlob;
                        })
                        .onFailure().invoke(throwable -> 
                                LOG.errorf(throwable, "Failed to hydrate blob %d/%d: drive=%s, object_key=%s",
                                        blobIndex, blobList.size(),
                                        blob.getStorageRef().getDriveName(),
                                        blob.getStorageRef().getObjectKey()));
                blobHydrationTasks.add(hydratedBlobUni);
            } else {
                // Already hydrated or no storage_ref, return as-is
                blobHydrationTasks.add(Uni.createFrom().item(blob));
            }
        }
        
        // Execute all hydration tasks in parallel and collect results
        // Using Uni.join() for type-safe List<Blob> instead of Uni.combine() which returns List<Object>
        return Uni.join().all(blobHydrationTasks).andFailFast().map(hydratedBlobsList -> {
                    // Rebuild the Blobs collection with hydrated blobs
                    Blobs.Builder blobsBuilder = Blobs.newBuilder();
                    for (Blob hydratedBlob : hydratedBlobsList) {
                        blobsBuilder.addBlob(hydratedBlob);
                    }
                    Blobs hydratedBlobs = blobsBuilder.build();

                    // Rebuild the BlobBag with hydrated Blobs
                    BlobBag hydratedBlobBag = doc.getBlobBag().toBuilder()
                            .setBlobs(hydratedBlobs)
                            .build();

                    // Rebuild the PipeDoc with hydrated BlobBag
                    return doc.toBuilder()
                            .setBlobBag(hydratedBlobBag)
                            .build();
                })
                .onFailure().invoke(throwable -> 
                        LOG.errorf(throwable, "Failed to hydrate multiple blobs: %d blobs total", blobList.size()));
    }

    /**
     * Calls the configured module for this node via dynamic gRPC.
     * <p>
     * Uses the DynamicGrpcClientFactory to create a client for the module's
     * gRPC service. The service name is resolved from the module definition
     * in the graph cache, falling back to the module ID if not specified.
     * <p>
     * Implements retry with exponential backoff for transient failures (UNAVAILABLE,
     * DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED). Non-retryable errors fail immediately.
     *
     * @param stream The input stream with document to process
     * @param node The node configuration specifying which module to call
     * @return A Uni that completes with the processed document output
     * @throws RuntimeException if module processing fails or returns error
     */
    private Uni<PipeDoc> callModule(PipeStream stream, GraphNode node) {
        String moduleId = node.getModuleId();

        // Note: stream should already be hydrated via ensureHydration() before this is called
        if (!stream.hasDocument()) {
            LOG.errorf("Cannot call module %s - stream %s does not have a document", moduleId, stream.getStreamId());
            return Uni.createFrom().failure(new IllegalStateException("Stream document must be hydrated before calling module"));
        }
        // Build ProcessConfiguration from GraphNode's custom_config
        ProcessConfiguration.Builder configBuilder = ProcessConfiguration.newBuilder();
        if (node.hasCustomConfig()) {
            ProcessConfiguration nodeConfig = node.getCustomConfig();
            if (nodeConfig.hasJsonConfig()) {
                configBuilder.setJsonConfig(nodeConfig.getJsonConfig());
            }
            configBuilder.putAllConfigParams(nodeConfig.getConfigParamsMap());
        }

        ProcessDataRequest request = ProcessDataRequest.newBuilder()
                .setDocument(stream.getDocument())
                .setConfig(configBuilder.build())
                .setMetadata(ServiceMetadata.newBuilder()
                        .setStreamId(stream.getStreamId())
                        .setCurrentHopNumber(stream.getHopCount())
                        .build())
                .build();

        // Resolve service name from graph cache (reactive) and call module
        return graphCache.getModule(moduleId)
            .map(moduleOpt -> {
                return moduleOpt
                    .map(ModuleDefinition::getGrpcServiceName)
                    .filter(s -> !s.isEmpty())
                    .orElse(moduleId);
            })
            .flatMap(serviceName -> grpcClientFactory.getClient(serviceName, MutinyPipeStepProcessorServiceGrpc::newMutinyStub)
                    .flatMap(stub -> stub.processData(request)))
                .onFailure(this::isRetryableFailure).retry()
                    .withBackOff(
                            Duration.ofMillis(moduleRetryInitialDelayMs),
                            Duration.ofMillis(moduleRetryMaxDelayMs))
                    .atMost(moduleRetryMaxAttempts)
                .onFailure().invoke(error -> {
                    if (isRetryableFailure(error)) {
                        LOG.warnf("Module %s failed after %d retry attempts: %s",
                                moduleId, moduleRetryMaxAttempts, error.getMessage());
                    } else {
                        LOG.debugf("Module %s failed with non-retryable error: %s",
                                moduleId, error.getMessage());
                    }
                })
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
     * Determines if a failure is retryable based on gRPC status codes.
     * <p>
     * Retryable status codes indicate transient failures that may succeed on retry:
     * <ul>
     *   <li>UNAVAILABLE - Service temporarily unavailable (e.g., connection refused)</li>
     *   <li>DEADLINE_EXCEEDED - Request timeout</li>
     *   <li>RESOURCE_EXHAUSTED - Rate limiting or quota exceeded</li>
     * </ul>
     *
     * @param throwable The failure to check
     * @return true if the failure should trigger a retry, false otherwise
     */
    private boolean isRetryableFailure(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
            Status.Code code = ((StatusRuntimeException) throwable).getStatus().getCode();
            return RETRYABLE_STATUS_CODES.contains(code);
        }
        // Also check for wrapped exceptions
        if (throwable.getCause() instanceof StatusRuntimeException) {
            Status.Code code = ((StatusRuntimeException) throwable.getCause()).getStatus().getCode();
            return RETRYABLE_STATUS_CODES.contains(code);
        }
        return false;
    }

    /**
     * Updates the stream metadata after successful module processing.
     * <p>
     * Creates execution history records, increments hop count, updates the
     * current node ID, and records the processing timestamp. This metadata
     * is used for observability, debugging, and routing decisions.
     * <p>
     * Captures execution timing (start_time, end_time) and the service instance ID
     * to support distributed tracing and performance analysis.
     *
     * @param stream The original input stream
     * @param outputDoc The processed document output from the module
     * @param node The node that performed the processing
     * @param startTime The instant when processing began (for duration calculation)
     * @return A new PipeStream instance with updated metadata
     */
    private PipeStream updateStreamMetadata(PipeStream stream, PipeDoc outputDoc, GraphNode node, Instant startTime) {
        Instant endTime = Instant.now();
        Timestamp startTimestamp = Timestamps.fromMillis(startTime.toEpochMilli());
        Timestamp endTimestamp = Timestamps.fromMillis(endTime.toEpochMilli());
        long durationMs = java.time.Duration.between(startTime, endTime).toMillis();

        LOG.debugf("Step %s completed in %d ms (instance: %s)",
                node.getName(), durationMs, serviceInstanceId);

        StepExecutionRecord history = StepExecutionRecord.newBuilder()
                .setStepName(node.getName())
                .setHopNumber(stream.getHopCount() + 1)
                .setServiceInstanceId(serviceInstanceId)
                .setStartTime(startTimestamp)
                .setEndTime(endTimestamp)
                .setStatus("SUCCESS")
                .build();

        return stream.toBuilder()
                .setDocument(outputDoc)
                .setHopCount(stream.getHopCount() + 1)
                .setCurrentNodeId(node.getNodeId())
                .addProcessingPath(node.getNodeId())
                .setMetadata(stream.getMetadata().toBuilder()
                        .setLastProcessedAt(endTimestamp)
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
        return graphCache.getOutgoingEdges(node.getNodeId())
            .flatMap(edges -> {
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
            });
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
            PipeDoc doc = nextStream.hasDocument() ? nextStream.getDocument() : null;
            if (doc == null) {
                LOG.warnf("Cannot dispatch stream %s - document is not hydrated", nextStream.getStreamId());
                return Uni.createFrom().failure(new IllegalStateException("Document must be hydrated before Kafka dispatch"));
            }
            
            // Use the current node ID as the graph_location_id when saving to repository
            // This identifies which graph node processed this document state
            String graphLocationId = nextStream.getCurrentNodeId();
            String accountId = nextStream.getMetadata().getAccountId();
            
            // Pass currentClusterId so the repository service can organize documents by cluster
            // This determines the S3 path structure: .../{clusterId}/{uuid}.pb
            return repoClient.savePipeDoc(doc, "default", graphLocationId, currentClusterId)
                .flatMap(repositoryNodeId -> {
                    // Create DocumentReference using the graph location ID (source_node_id in proto)
                    // This allows the next node to retrieve the document via GetPipeDocByReference
                    DocumentReference docRef = DocumentReference.newBuilder()
                            .setDocId(doc.getDocId())
                            .setSourceNodeId(graphLocationId)  // proto field name is source_node_id, represents graph_address_id
                            .setAccountId(accountId)
                            .build();
                            
                    PipeStream refStream = nextStream.toBuilder()
                            .clearDocument()
                            .setDocumentRef(docRef)
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
        return graphCache.getEntryNodeId(request.getDatasourceId())
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