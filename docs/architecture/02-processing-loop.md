# Engine Processing Loop

## Core Loop

```java
void processNode(PipeStream stream) {
    String nodeId = stream.getCurrentNodeId();
    
    // 1. Lookup node config from graph cache
    NodeConfig config = graphCache.getNode(nodeId);
    
    // 2. Hydrate PipeDoc if reference-only
    PipeDoc doc = stream.hasDocument() 
        ? stream.getDocument()
        : repoService.getPipeDoc(stream.getDocumentRef());
    
    // 3. Filter check (CEL)
    if (config.hasFilterCondition()) {
        boolean shouldProcess = celEvaluate(config.getFilterCondition(), doc);
        if (!shouldProcess) {
            // Skip this node, route directly to next
            routeToNextNodes(stream, doc);
            return;
        }
    }
    
    // 4. Apply pre-mappings
    doc = applyMappings(doc, config.getPreMappingsList());
    
    // 5. Call remote module
    ProcessDataResponse response = callModule(nodeId, doc, config);
    
    if (!response.getSuccess()) {
        // Log error in history, continue (not DLQ for logical failures)
        appendError(stream, response.getMessage());
    }
    
    PipeDoc outputDoc = response.getOutputDoc();
    
    // 6. Apply post-mappings
    outputDoc = applyMappings(outputDoc, config.getPostMappingsList());
    
    // 7. Update stream metadata
    stream = updateStreamMetadata(stream, outputDoc, nodeId);
    
    // 8. Route to next nodes
    routeToNextNodes(stream, outputDoc);
}
```

## Stream Metadata Updates

After each hop, the engine updates:

```java
PipeStream updateStreamMetadata(PipeStream stream, PipeDoc doc, String nodeId) {
    return stream.toBuilder()
        // Replace document
        .setDocument(doc)
        // Increment hop
        .setHopCount(stream.getHopCount() + 1)
        // Append to history
        .setMetadata(stream.getMetadata().toBuilder()
            .addHistory(StepExecutionRecord.newBuilder()
                .setNodeId(nodeId)
                .setTimestamp(now())
                .setDurationMs(processingTime)
                .setStatus(SUCCESS)
                .build())
            .setLastProcessedAt(now())
            .build())
        // Append to path
        .addProcessingPath(nodeId)
        .build();
}
```

## Routing to Next Nodes

```java
void routeToNextNodes(PipeStream stream, PipeDoc doc) {
    String currentNodeId = stream.getCurrentNodeId();
    List<GraphEdge> edges = graphCache.getOutgoingEdges(currentNodeId);
    
    // Sort by priority
    edges.sort(Comparator.comparingInt(GraphEdge::getPriority));
    
    // Find all matching edges (fan-out)
    List<GraphEdge> matchingEdges = new ArrayList<>();
    for (GraphEdge edge : edges) {
        if (!edge.hasCondition() || celEvaluate(edge.getCondition(), doc)) {
            matchingEdges.add(edge);
        }
    }
    
    if (matchingEdges.isEmpty()) {
        // Terminal node - no outgoing edges
        handleTerminalNode(stream, doc);
        return;
    }
    
    // Route to each matching edge
    for (GraphEdge edge : matchingEdges) {
        PipeStream clonedStream = stream.toBuilder()
            .setCurrentNodeId(edge.getToNodeId())
            .build();
        
        routeViaTransport(clonedStream, doc, edge);
    }
}

void routeViaTransport(PipeStream stream, PipeDoc doc, GraphEdge edge) {
    if (edge.getTransportType() == TransportType.MESSAGING) {
        // Kafka path - offload doc to S3
        String docRef = repoService.savePipeDoc(doc);
        PipeStream streamWithRef = stream.toBuilder()
            .clearDocument()
            .setDocumentRef(docRef)
            .build();
        
        String topic = edge.hasKafkaTopic() 
            ? edge.getKafkaTopic()
            : graphCache.getNode(edge.getToNodeId()).getKafkaInputTopic();
        
        kafkaProducer.send(topic, streamWithRef);
        
    } else if (edge.getTransportType() == TransportType.GRPC) {
        // gRPC path - inline doc
        if (edge.getIsCrossCluster()) {
            engineClient.routeToCluster(
                edge.getToClusterId(), 
                edge.getToNodeId(), 
                stream.toBuilder().setDocument(doc).build());
        } else {
            // Local recursive call or queue
            processNode(stream.toBuilder()
                .setCurrentNodeId(edge.getToNodeId())
                .setDocument(doc)
                .build());
        }
    }
}
```

## Calling Remote Modules

```java
ProcessDataResponse callModule(String nodeId, PipeDoc doc, NodeConfig config) {
    // Lookup module endpoint from Consul
    String moduleName = config.getModuleId();
    ServiceInstance instance = consul.getHealthyInstance(moduleName);
    
    if (instance == null) {
        // Module unavailable - DLQ or retry
        throw new ModuleUnavailableException(moduleName);
    }
    
    // Build request
    ProcessDataRequest request = ProcessDataRequest.newBuilder()
        .setDocument(doc)
        .setConfig(ProcessConfiguration.newBuilder()
            .setCustomJsonConfig(config.getCustomConfig())
            .build())
        .setMetadata(ServiceMetadata.newBuilder()
            .setStreamId(currentStream.getStreamId())
            .setCurrentHopNumber(currentStream.getHopCount())
            .build())
        .build();
    
    // gRPC call with timeout
    ManagedChannel channel = getOrCreateChannel(instance);
    PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub stub = 
        PipeStepProcessorServiceGrpc.newBlockingStub(channel)
            .withDeadlineAfter(config.getTimeout(), TimeUnit.MILLISECONDS);
    
    return stub.processData(request);
}
```

## Error Handling

| Error Type | Handling |
|------------|----------|
| Module returns `success=false` | Log in history, continue to next nodes |
| Module unreachable (Consul down) | Route to DLQ |
| gRPC timeout | Retry with backoff, then DLQ |
| CEL evaluation error | Log error, skip edge |
| S3 hydration failure | Retry, then DLQ |
