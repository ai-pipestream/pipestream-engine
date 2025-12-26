# Transport & Routing

## Overview

**gRPC is the primary transport.** Most document processing flows through synchronous gRPC calls:

```
Intake ──gRPC──► Engine ──gRPC──► Module ──gRPC──► Engine ──gRPC──► Module ──gRPC──► ...
```

**Kafka is for specific edges** where asynchronous, buffered, or cross-cluster transport is needed.

## Transport Types

Transport is defined on **edges**, not nodes. Each edge specifies how to reach its target:

| Transport | Proto Value | Use Case | Behavior |
|-----------|-------------|----------|----------|
| **gRPC** | `TRANSPORT_TYPE_GRPC` | Default, real-time, most processing | Direct call, low latency |
| **Kafka** | `TRANSPORT_TYPE_MESSAGING` | Cross-cluster, async buffering, batch | Buffered, requires persistence |

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
  optional string kafka_topic = 9;  // Override default if Kafka
  
  // Loop prevention
  int32 max_hops = 10;
}
```

## gRPC Transport (Primary)

```
┌─────────────────────────────────────────────────────────────────┐
│                     gRPC FLOW (TYPICAL)                          │
│                                                                  │
│  Engine ────gRPC call────► Module                               │
│     │                         │                                  │
│     │   PipeDoc inline        │   PipeDoc inline                │
│     │   (no persistence)      │   (no persistence)              │
│     │                         │                                  │
│     │◄────gRPC response───────┘                                  │
│     │                                                            │
│     └────gRPC call────► Next Engine/Module                      │
│                                                                  │
│  Benefits:                                                       │
│  • Low latency (no Kafka hop)                                   │
│  • No mandatory persistence                                     │
│  • Simple request/response                                      │
│  • 2GB payload limit (plenty for most docs)                     │
└─────────────────────────────────────────────────────────────────┘
```

**When to use gRPC:**
- Default for all edges unless Kafka is specifically needed
- Latency-sensitive processing
- Small to medium documents
- Same-cluster routing

## Kafka Transport (Specific Edges)

```
┌─────────────────────────────────────────────────────────────────┐
│                     KAFKA FLOW                                   │
│                                                                  │
│  Engine ──► Repo.SavePipeDoc() ──► S3                           │
│     │                                                            │
│     └──► Kafka.publish(topic, PipeStream with document_ref)     │
│                           │                                      │
│                           ▼                                      │
│                    ┌──────────────┐                             │
│                    │    Kafka     │                             │
│                    │   (buffered) │                             │
│                    └──────────────┘                             │
│                           │                                      │
│                           ▼                                      │
│  Target Engine ◄── Kafka.consume()                              │
│     │                                                            │
│     └──► Repo.GetPipeDoc(document_ref) ──► S3                   │
│                                                                  │
│  Constraints:                                                    │
│  • 10MB message limit → MUST use document_ref                   │
│  • Requires persistence before publish                          │
│  • Adds latency (Kafka + S3 round trips)                        │
└─────────────────────────────────────────────────────────────────┘
```

**When to use Kafka:**
- Cross-cluster routing
- Batch ingestion buffering
- Decoupling (producer doesn't wait for consumer)
- Replay/reprocessing capability needed
- Large documents (over gRPC comfort zone)

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
```

### Routing Algorithm

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

## Routing by Transport Type

```java
void routeToNextNode(PipeStream stream, PipeDoc doc, GraphEdge edge) {
    String accountId = stream.getMetadata().getAccountId();
    String currentNodeId = stream.getCurrentNodeId();
    
    if (edge.getTransportType() == TRANSPORT_TYPE_MESSAGING) {
        // KAFKA: Must persist, send reference
        repoService.savePipeDoc(doc, currentNodeId, accountId);
        
        PipeStream dehydrated = stream.toBuilder()
            .clearDocument()
            .setDocumentRef(DocumentReference.newBuilder()
                .setDocId(doc.getDocId())
                .setSourceNodeId(currentNodeId)
                .setAccountId(accountId)
                .build())
            .setCurrentNodeId(edge.getToNodeId())
            .build();
        
        String topic = edge.hasKafkaTopic() 
            ? edge.getKafkaTopic()
            : graphCache.getNode(edge.getToNodeId()).getKafkaInputTopic();
        
        kafkaProducer.send(topic, dehydrated);
        
    } else {
        // GRPC: Can pass inline (default) or reference (large docs)
        PipeStream next = stream.toBuilder()
            .setCurrentNodeId(edge.getToNodeId())
            .setDocument(doc)
            .setHopCount(stream.getHopCount() + 1)
            .build();
        
        if (edge.getIsCrossCluster()) {
            engineClient.routeToCluster(edge.getToClusterId(), next);
        } else {
            processNode(next);  // Local recursive call or queue
        }
    }
}
```

## Topic Naming Convention

```
{environment}.{cluster}.{node-uuid}

Examples:
prod.us-east.abc123-def456-parser-node
prod.us-east.xyz789-chunker-node
staging.default.test-embedder-node
```

## Cross-Cluster Routing

When `edge.is_cross_cluster = true`:

```java
void routeCrossCluster(PipeStream stream, PipeDoc doc, GraphEdge edge) {
    // Always use Kafka for cross-cluster (reliability + decoupling)
    // Same as Kafka transport above, but target topic is in different cluster
    
    String bridgeTopic = edge.hasKafkaTopic()
        ? edge.getKafkaTopic()
        : "bridge." + edge.getToClusterId() + "." + edge.getToNodeId();
    
    // ... persist and publish
}
```
