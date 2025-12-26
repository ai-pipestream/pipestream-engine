# Engine Processing Loop

The Engine Processing Loop is the heart of the Pipestream orchestration. It manages the lifecycle of a document as it moves through a specific node in the pipeline graph, handling everything from data preparation to remote module execution and routing to subsequent nodes.

## Core Loop

The core loop orchestrates the execution of a single pipeline node. It fetches the necessary configuration, prepares the document data, evaluates whether the node should be executed, calls the remote module, and finally routes the result to the next steps in the graph.

### Processing Steps
- **Node Configuration Lookup**: Retrieves the specific settings for the current node from the in-memory graph cache.
- **Document Hydration**: Ensures the full document data is available, fetching it from the repository service if only a reference was provided.
- **Filter Evaluation**: Checks a CEL (Common Expression Language) condition to decide if the node's logic should be applied or if the document should bypass this node.
- **Pre-mapping Transforms**: Applies data transformations to the document before it is sent to the module.
- **Module Execution**: Calls the remote module service via gRPC and handles the response.
- **Post-mapping Transforms**: Applies further transformations to the output document received from the module.
- **Metadata Update**: Records the execution history and updates stream-level information.
- **Routing**: Determines the next nodes in the pipeline and dispatches the stream.

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

```mermaid
flowchart TD
    Start([Start processNode]) --> Config[Lookup Node Config]
    Config --> Hydrate{Hydrate Doc?}
    Hydrate -- Ref only --> Fetch[Fetch from Repo Service]
    Hydrate -- Inline --> Filter{Filter CEL?}
    Fetch --> Filter
    Filter -- Match --> PreMap[Apply Pre-mappings]
    Filter -- No Match --> Route[Route to Next Nodes]
    PreMap --> Call[Call Remote Module]
    Call --> Success{Success?}
    Success -- No --> Error[Append Error to History]
    Success -- Yes --> PostMap[Apply Post-mappings]
    Error --> PostMap
    PostMap --> Meta[Update Stream Metadata]
    Meta --> Route
    Route --> End([End])
```

## Stream Metadata Updates

Metadata updates ensure that every step of the document's journey is tracked and that the stream state correctly reflects its current position and history.

### Metadata Update Actions
- **Document Replacement**: Updates the stream with the latest version of the document (the output from the node).
- **Hop Count Increment**: Tracks the number of nodes the document has passed through.
- **Execution History Logging**: Adds a `StepExecutionRecord` containing the node ID, timestamp, processing duration, and status.
- **Processing Path Tracking**: Appends the current node ID to the ordered list of visited nodes.

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

```mermaid
graph LR
    Input[Original PipeStream] --> Builder[Stream Builder]
    subgraph Updates [Field Updates]
        Builder --> Doc[Set Document]
        Builder --> Hop[Hop Count + 1]
        Builder --> Hist[Add History Record]
        Builder --> Path[Add Node to Path]
    end
    Updates --> Output[Updated PipeStream]
```

## Routing to Next Nodes

Routing manages the "fan-out" logic of the pipeline. It determines which edges to follow based on priority and conditional logic, and then chooses the appropriate transport mechanism (gRPC or Kafka) to deliver the document.

### Routing Logic
- **Edge Retrieval**: Gets all outgoing connections for the current node from the graph cache.
- **Priority Sorting**: Ensures edges are evaluated in the defined order.
- **Condition Evaluation**: Uses CEL to determine which edges the document qualifies for.
- **Terminal Handling**: Finalizes the stream if no matching outgoing edges are found.
- **Transport Selection**:
    - **Kafka Path**: Offloads large documents to storage (S3) and sends a reference via a Kafka topic.
    - **gRPC Path**: Sends the document directly to another engine instance, either in a different cluster or via a local recursive call.

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

```mermaid
flowchart TD
    FindEdges[Get Outgoing Edges] --> Sort[Sort by Priority]
    Sort --> Eval{Evaluate CEL Conditions}
    Eval --> None{Any matches?}
    None -- No --> Terminal[Handle Terminal Node]
    None -- Yes --> Loop[For Each Matching Edge]
    Loop --> Transport{Transport Type?}
    Transport -- Messaging/Kafka --> S3[Save to S3/Repo]
    S3 --> Kafka[Send to Kafka Topic]
    Transport -- gRPC --> Cluster{Cross Cluster?}
    Cluster -- Yes --> Remote[Route to Remote Cluster]
    Cluster -- No --> Local[Local processNode call]
    Kafka --> LoopEnd[Next Edge]
    Remote --> LoopEnd
    Local --> LoopEnd
    LoopEnd --> Loop
```

## Calling Remote Modules

The engine communicates with external modules (like parsers or LLMs) via gRPC. This process includes discovering healthy service instances and building a standard request structure.

### Module Call Steps
- **Service Discovery**: Queries Consul to find a healthy instance of the required module service.
- **Availability Check**: Ensures a service instance exists; otherwise, throws an exception for retry/DLQ handling.
- **Request Construction**: Packages the document, node configuration, and stream metadata into a `ProcessDataRequest`.
- **gRPC Invocation**: Executes the remote call using a blocking stub with a configurable deadline/timeout.

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

```mermaid
sequenceDiagram
    participant E as Engine
    participant C as Consul
    participant M as Remote Module
    
    E->>C: Get healthy instance for Module ID
    C-->>E: Service Instance (Host/Port)
    alt Instance not found
        E-->>E: Throw ModuleUnavailableException
    end
    E->>E: Build ProcessDataRequest
    E->>M: gRPC: processData(request)
    Note over E,M: With Timeout (Deadline)
    M-->>E: ProcessDataResponse
```

## Error Handling

The engine employs different strategies depending on the nature of the failure to ensure robustness and observability.

| Error Type | Handling |
|------------|----------|
| Module returns `success=false` | Log in history, continue to next nodes |
| Module unreachable (Consul down) | Route to DLQ |
| gRPC timeout | Retry with backoff, then DLQ |
| CEL evaluation error | Log error, skip edge |
| S3 hydration failure | Retry, then DLQ |
