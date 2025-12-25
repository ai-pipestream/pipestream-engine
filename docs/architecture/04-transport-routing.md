# Transport & Routing

## Transport Types

Transport is defined on **edges**, not nodes. Each edge specifies how to send to its target:

| Transport | Proto Value | Use Case | Behavior |
|-----------|-------------|----------|----------|
| **Kafka** | `TRANSPORT_TYPE_MESSAGING` | Async, batch, cost-sensitive | Buffered, rewindable |
| **gRPC** | `TRANSPORT_TYPE_GRPC` | Real-time, latency-sensitive | Direct call |

## Edge Definition

```protobuf
message GraphEdge {
  string edge_id = 1;
  string from_node_id = 2;
  string to_node_id = 3;
  
  // Cross-cluster routing
  optional string to_cluster_id = 4;
  bool is_cross_cluster = 5;
  
  // Routing logic (CEL)
  string condition = 6;
  int32 priority = 7;
  
  // Transport
  TransportType transport_type = 8;
  optional string kafka_topic = 9;  // Override default
  
  // Loop prevention
  int32 max_hops = 10;
}
```

## Kafka Transport (Async)

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│ Engine  │────▶│   S3    │────▶│  Kafka  │────▶│ Engine  │
│ (sender)│     │(offload)│     │ (topic) │     │(receiver)│
└─────────┘     └─────────┘     └─────────┘     └─────────┘
```

**Flow:**
1. Engine offloads PipeDoc to S3 via Repo Service
2. PipeStream (with S3 reference, not inline doc) published to Kafka
3. Target engine consumes from topic
4. Target engine hydrates PipeDoc from S3

**Benefits:**
- Kafka messages stay small (just metadata + S3 ref)
- Natural backpressure via consumer lag
- Rewindable - replay from any offset
- Decoupled - sender doesn't wait for receiver

**S3 Key Convention:**
```
s3://{bucket}/streams/{account_id}/{stream_id}/{node_id}.pb
```

## gRPC Transport (Sync)

```
┌─────────┐                      ┌─────────┐
│ Engine  │──────gRPC call──────▶│ Engine  │
│ (sender)│      (inline doc)    │(receiver)│
└─────────┘                      └─────────┘
```

**Flow:**
1. Engine calls target engine's `ProcessNode` RPC directly
2. PipeDoc sent inline (or S3 ref for large payloads)
3. Sender waits for response

**Benefits:**
- Lower latency (no Kafka hop)
- Immediate feedback on success/failure
- Simpler for real-time paths

**Drawbacks:**
- No replay capability
- Sender blocked during processing
- Tight coupling

## CEL Conditional Routing

Edge conditions use [CEL (Common Expression Language)](https://github.com/google/cel-spec):

```cel
// Route PDFs to parser
doc.search_metadata.document_type == "PDF"

// Route large files to async processing
doc.blob_bag.blob.size_bytes > 10000000

// Route by source
doc.search_metadata.source_uri.startsWith("s3://sensitive-bucket/")

// Check field existence
has(doc.search_metadata.doi)

// Route by tag
"legal" in doc.search_metadata.tags.tag_data

// Complex condition
doc.search_metadata.mime_type == "application/pdf" && 
doc.search_metadata.content_length > 1000000
```

### CEL Evaluation

```java
public class CelEvaluator {
    private final CelCompiler compiler;
    private final CelRuntime runtime;
    
    public CelEvaluator() {
        this.compiler = CelCompilerFactory.standardCelCompilerBuilder()
            .addMessageTypes(PipeDoc.getDescriptor())
            .addVar("doc", StructTypeReference.create(PipeDoc.getDescriptor().getFullName()))
            .build();
        this.runtime = CelRuntimeFactory.standardCelRuntimeBuilder().build();
    }
    
    public CelProgram compile(String expression) {
        CelAbstractSyntaxTree ast = compiler.compile(expression).getAst();
        return runtime.createProgram(ast);
    }
    
    public boolean evaluate(CelProgram program, PipeDoc doc) {
        return (Boolean) program.eval(Map.of("doc", doc));
    }
}
```

## Routing Algorithm

```java
List<GraphEdge> resolveMatchingEdges(String nodeId, PipeDoc doc) {
    List<GraphEdge> edges = graphCache.getOutgoingEdges(nodeId);
    
    // Sort by priority (lower = higher priority)
    edges.sort(Comparator.comparingInt(GraphEdge::getPriority));
    
    List<GraphEdge> matching = new ArrayList<>();
    
    for (GraphEdge edge : edges) {
        // Empty condition = always match
        if (!edge.hasCondition()) {
            matching.add(edge);
            continue;
        }
        
        // Evaluate CEL condition
        CelProgram program = graphCache.getCompiledCondition(edge.getEdgeId());
        if (celEvaluator.evaluate(program, doc)) {
            matching.add(edge);
        }
    }
    
    return matching;  // Fan-out to ALL matching edges
}
```

## Topic Management

Engine dynamically manages Kafka subscriptions:

```java
void onGraphUpdate(PipelineGraph graph) {
    Set<String> requiredTopics = new HashSet<>();
    
    // Collect all topics this engine should consume
    for (GraphNode node : graph.getNodesList()) {
        if (shouldHandleNode(node)) {
            requiredTopics.add(node.getKafkaInputTopic());
        }
    }
    
    // Diff with current subscriptions
    Set<String> toSubscribe = difference(requiredTopics, currentTopics);
    Set<String> toUnsubscribe = difference(currentTopics, requiredTopics);
    
    // Update subscriptions
    kafkaConsumer.subscribe(toSubscribe);
    kafkaConsumer.unsubscribe(toUnsubscribe);
    
    currentTopics = requiredTopics;
}
```

## Cross-Cluster Routing

When `edge.is_cross_cluster = true`:

```java
void routeCrossCluster(PipeStream stream, PipeDoc doc, GraphEdge edge) {
    // Always use Kafka for cross-cluster (reliability)
    String bridgeTopic = "bridge." + edge.getToClusterId() + "." + edge.getToNodeId();
    
    // Offload doc
    String docRef = repoService.savePipeDoc(doc);
    
    // Publish with cluster routing metadata
    PipeStream crossClusterStream = stream.toBuilder()
        .clearDocument()
        .setDocumentRef(docRef)
        .setClusterId(edge.getToClusterId())
        .setCurrentNodeId(edge.getToNodeId())
        .build();
    
    kafkaProducer.send(bridgeTopic, crossClusterStream);
}
```
