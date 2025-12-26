# Transport & Routing

## Overview

**gRPC is the primary transport.** Most document processing flows through synchronous gRPC calls:

```
Intake ──gRPC──► Engine ──gRPC──► Module ──gRPC──► Engine ──gRPC──► Module ──gRPC──► ...
```

**Kafka is for specific edges** where asynchronous, buffered, or replay capability is needed.

## One Transport Per Edge

Each edge in the graph chooses ONE transport - no mixing:

| Transport | Flow | Use Case |
|-----------|------|----------|
| **gRPC** | Engine → Engine (direct) | Fast path, low latency |
| **Kafka** | Engine → Repo → Kafka → Sidecar → Engine | Async, replayable |

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
  
  // Transport selection
  TransportType transport_type = 8;    // GRPC or MESSAGING
  optional string kafka_topic = 9;     // Override default if Kafka
  
  // Loop prevention
  int32 max_hops = 10;
}
```

## gRPC Transport (Fast Path)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     gRPC FLOW (DEFAULT)                                          │
│                                                                                  │
│  Engine A ────────gRPC call────────► Engine B                                   │
│      │                                    │                                      │
│      │   PipeStream with inline PipeDoc   │                                      │
│      │   No persistence required          │                                      │
│      │   Immediate processing             │                                      │
│      │                                    │                                      │
│  Benefits:                                                                       │
│  • Lowest latency (no Kafka hop)                                                │
│  • No mandatory persistence                                                     │
│  • Simple request/response                                                      │
│  • 2GB payload limit (plenty for most docs)                                     │
│                                                                                  │
│  Limitations:                                                                   │
│  • No replay capability                                                         │
│  • Caller waits for response                                                    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Kafka Transport (Async/Replay Path)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     KAFKA FLOW                                                   │
│                                                                                  │
│  Engine A                                                                        │
│      │                                                                           │
│      ├──► Repo.SavePipeDoc() ──► S3 (persist document)                          │
│      │                                                                           │
│      └──► Kafka.publish(topic, PipeStream with document_ref)                    │
│                          │                                                       │
│                          ▼                                                       │
│                    ┌──────────┐                                                  │
│                    │  Kafka   │  (10MB limit, so only document_ref)             │
│                    └──────────┘                                                  │
│                          │                                                       │
│                          ▼                                                       │
│                    ┌──────────┐                                                  │
│                    │ Sidecar  │  (Consul lease for this topic)                  │
│                    │ • Consume│                                                  │
│                    │ • Hydrate│──► Repo.GetPipeDoc() ──► S3                     │
│                    └──────────┘                                                  │
│                          │                                                       │
│                          ▼ localhost gRPC                                        │
│                    ┌──────────┐                                                  │
│                    │ Engine B │  (receives hydrated PipeStream)                 │
│                    └──────────┘                                                  │
│                                                                                  │
│  Benefits:                                                                       │
│  • Replay capability (rewind offset)                                            │
│  • Async (fire and forget from Engine A's perspective)                          │
│  • Natural buffering during load spikes                                         │
│  • Cross-cluster routing                                                        │
│                                                                                  │
│  Limitations:                                                                   │
│  • Higher latency (Kafka + S3 round trips)                                      │
│  • Requires persistence                                                         │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

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
            .setHopCount(stream.getHopCount() + 1)
            .build();
        
        String topic = edge.hasKafkaTopic() 
            ? edge.getKafkaTopic()
            : getDefaultTopic(edge.getToNodeId());
        
        kafkaProducer.send(topic, dehydrated);
        
    } else {
        // GRPC: Pass inline, no persistence required
        PipeStream next = stream.toBuilder()
            .setCurrentNodeId(edge.getToNodeId())
            .setDocument(doc)
            .setHopCount(stream.getHopCount() + 1)
            .build();
        
        if (edge.getIsCrossCluster()) {
            engineClient.routeToCluster(edge.getToClusterId(), next);
        } else {
            // Direct gRPC call to engine handling that node
            engineClient.processNode(next);
        }
    }
}
```

## Topic Naming Convention

```
pipestream.{cluster-id}.{node-uuid}

Examples:
pipestream.prod-us-east.abc123-def456-parser-node
pipestream.prod-us-east.xyz789-chunker-node
pipestream.staging.test-embedder-node

DLQ:
dlq.{cluster-id}.{node-uuid}
```

## Graph Node Creates Topics

When a node is created that has incoming Kafka edges:

```java
void onNodeCreated(GraphNode node) {
    if (hasIncomingKafkaEdges(node)) {
        String topicName = "pipestream." + node.getClusterId() + "." + node.getNodeId();
        kafkaAdmin.createTopic(topicName, partitions, replicationFactor);
        
        String dlqTopic = "dlq." + node.getClusterId() + "." + node.getNodeId();
        kafkaAdmin.createTopic(dlqTopic, partitions, replicationFactor);
        
        // Register in Consul for sidecar lease distribution
        consul.kvPut("pipestream/topics/" + node.getNodeId(), "");
    }
}
```
