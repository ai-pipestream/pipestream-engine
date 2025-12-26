# Kafka Sidecar Pattern

## Overview

The Kafka Sidecar separates Kafka consumption from engine processing. This keeps the engine as a **pure gRPC service** while enabling async/replay capabilities via Kafka.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     COMPUTE TASK (Fargate/Container)                             │
│                                                                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │  Consul Agent   │    │  Kafka Sidecar  │    │     Engine      │             │
│  │                 │    │                 │    │                 │             │
│  │  • Health check │◄───│  • Lease mgmt   │    │  • Pure gRPC    │             │
│  │  • Service reg  │    │  • Consume      │───►│  • Stateless    │             │
│  │                 │    │  • Hydrate S3   │    │  • Routes/maps  │             │
│  │                 │    │  • Commit offset│◄───│  • Calls modules│             │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘             │
│                                │                       │                        │
│                                │ localhost:50051       │ gRPC to modules        │
│                                └───────────────────────┘                        │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Why Sidecar?

### The Problem

```
Without sidecar:
- 1000s of topics (one per node)
- Each engine joins every consumer group
- 100 engines × 1000 topics = 100,000 consumer group memberships
- Rebalancing nightmare
- Complex lease management inside engine
```

### The Solution

```
With sidecar:
- Engine is pure gRPC (doesn't know Kafka exists)
- Sidecar handles all Kafka complexity
- Consul leases distribute topics across sidecars
- Clean separation of concerns
- Scale independently
```

## Component Responsibilities

| Component | Responsibility | Kafka Aware? |
|-----------|----------------|---------------|
| **Kafka Sidecar** | Lease, consume, hydrate, deliver, commit | Yes |
| **Engine** | Filter, map, call modules, route | No |
| **Modules** | Transform PipeDoc | No |
| **Consul** | Health checks, lease management | N/A |

## Sidecar Responsibilities

### 1. Consul Lease Management

```java
public class TopicLeaseManager {
    private final ConsulClient consul;
    private final String sessionId;
    private final Set<String> leasedTopics = new ConcurrentHashSet<>();
    
    public void start() {
        // Create session linked to health check
        sessionId = consul.createSession(Session.builder()
            .name("kafka-sidecar-" + instanceId)
            .ttl("30s")
            .behavior("delete")  // Release leases on session death
            .checks(List.of(healthCheckId))
            .build());
        
        // Watch for available topics
        consul.watch("pipestream/topics/", this::onTopicsChanged);
    }
    
    void onTopicsChanged(List<String> availableTopics) {
        for (String topicId : availableTopics) {
            if (leasedTopics.size() < MAX_TOPICS_PER_SIDECAR) {
                tryAcquireLease(topicId);
            }
        }
    }
    
    void tryAcquireLease(String topicId) {
        boolean acquired = consul.kvAcquire(
            "pipestream/topics/" + topicId,
            sessionId
        );
        
        if (acquired) {
            leasedTopics.add(topicId);
            kafkaConsumer.subscribe(topicId);
        }
    }
    
    // On session invalidation (health check fail, crash):
    // Consul automatically releases all leases
    // Other sidecars can acquire them
}
```

### 2. Kafka Consumption

```java
public class KafkaConsumerLoop {
    
    void consumeLoop() {
        while (running) {
            ConsumerRecords<String, PipeStream> records = consumer.poll(Duration.ofMillis(100));
            
            for (ConsumerRecord<String, PipeStream> record : records) {
                try {
                    processRecord(record);
                    consumer.commitSync(Map.of(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                    ));
                } catch (Exception e) {
                    handleFailure(record, e);
                }
            }
        }
    }
}
```

### 3. Hydration

```java
void processRecord(ConsumerRecord<String, PipeStream> record) {
    PipeStream stream = record.value();
    
    // Hydrate: document_ref → document
    PipeDoc doc;
    if (stream.hasDocumentRef()) {
        DocumentReference ref = stream.getDocumentRef();
        doc = repoService.getPipeDoc(
            ref.getDocId(), 
            ref.getSourceNodeId(), 
            ref.getAccountId()
        );
    } else {
        doc = stream.getDocument();
    }
    
    // Build hydrated stream
    PipeStream hydrated = stream.toBuilder()
        .clearDocumentRef()
        .setDocument(doc)
        .build();
    
    // Forward to engine
    deliverToEngine(hydrated);
}
```

### 4. Delivery to Engine

```java
void deliverToEngine(PipeStream stream) {
    // Call localhost engine via gRPC
    ProcessNodeResponse response = engineStub.processNode(
        ProcessNodeRequest.newBuilder()
            .setStream(stream)
            .build()
    );
    
    if (!response.getSuccess()) {
        throw new ProcessingException(response.getErrorMessage());
    }
    
    // Success - caller commits offset
}
```

### 5. Failure Handling

```java
void handleFailure(ConsumerRecord<String, PipeStream> record, Exception e) {
    int retryCount = getRetryCount(record);
    
    if (retryCount < MAX_RETRIES) {
        // Retry with backoff
        scheduleRetry(record, retryCount + 1);
    } else {
        // Send to DLQ
        sendToDlq(record, e);
        // Commit offset to move past poison message
        consumer.commitSync(...);
    }
}
```

## Engine Stays Pure

The engine doesn't know or care where requests come from:

```java
@GrpcService
public class EngineService {
    
    // Same handler for:
    // - Direct gRPC from Intake
    // - Direct gRPC from another Engine
    // - Sidecar-delivered Kafka message
    
    public Uni<ProcessNodeResponse> processNode(ProcessNodeRequest request) {
        PipeStream stream = request.getStream();
        
        // Already hydrated (by sidecar or inline from gRPC)
        PipeDoc doc = stream.getDocument();
        
        // Process: filter, map, call module, route
        return processAndRoute(stream, doc);
    }
}
```

## Edge Transport Selection

One transport per edge - no mixing:

| Edge Transport | Flow | Use Case |
|----------------|------|----------|
| **gRPC** | Engine → Engine (direct) | Fast path, no replay |
| **Kafka** | Engine → Repo → Kafka → Sidecar → Engine | Async, replayable |

### gRPC Edge (Fast Path)

```
Engine A ───gRPC───► Engine B
    │                   │
    └── No Kafka        └── Immediate processing
        No S3               No replay capability
        Lowest latency
```

### Kafka Edge (Async/Replay Path)

```
Engine A ───► Repo.SavePipeDoc() ───► S3
    │
    └───► Kafka.publish(topic, document_ref)
                    │
                    ▼
              ┌──────────┐
              │  Kafka   │
              └──────────┘
                    │
                    ▼
              ┌──────────┐
              │ Sidecar  │
              │ (hydrate)│
              └──────────┘
                    │
                    ▼ localhost gRPC
              ┌──────────┐
              │ Engine B │
              └──────────┘
```

## Consul Lease Distribution

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     CONSUL LEASE DISTRIBUTION                                    │
│                                                                                  │
│  consul/pipestream/topics/                                                      │
│    ├── node-uuid-001 → sidecar-A (session-123)                                 │
│    ├── node-uuid-002 → sidecar-B (session-456)                                 │
│    ├── node-uuid-003 → sidecar-A (session-123)                                 │
│    ├── node-uuid-004 → sidecar-C (session-789)                                 │
│    └── ...                                                                      │
│                                                                                  │
│  Each sidecar holds leases for N topics                                         │
│  On sidecar death: session expires, leases auto-release                         │
│  Other sidecars acquire released topics                                         │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Graph Node → Topic Mapping

When a graph node is created with Kafka edges:

```java
void onNodeCreated(GraphNode node) {
    // Create topic for this node
    String topicName = "pipestream." + node.getClusterId() + "." + node.getNodeId();
    kafkaAdmin.createTopic(topicName, partitions, replicationFactor);
    
    // Create DLQ topic
    String dlqTopic = "dlq." + node.getClusterId() + "." + node.getNodeId();
    kafkaAdmin.createTopic(dlqTopic, partitions, replicationFactor);
    
    // Register in Consul for lease distribution
    consul.kvPut("pipestream/topics/" + node.getNodeId(), "");
}

void onNodeDeleted(GraphNode node) {
    // Remove from Consul (releases any lease)
    consul.kvDelete("pipestream/topics/" + node.getNodeId());
    
    // Optionally delete Kafka topics (or retain for replay)
}
```

## Scaling Model

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     SCALING                                                      │
│                                                                                  │
│  Scale based on:                                                                │
│  ├── gRPC load (CPU, latency) → add more tasks                                  │
│  └── Kafka lag → sidecars auto-acquire more topics                              │
│                                                                                  │
│  Each task contains:                                                            │
│  ├── Engine (handles gRPC from any source)                                      │
│  └── Sidecar (acquires available topic leases)                                  │
│                                                                                  │
│  Natural load balancing:                                                        │
│  - New task starts → sidecar acquires unleased topics                           │
│  - Task dies → Consul releases leases → other sidecars acquire                  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Benefits

| Benefit | Description |
|---------|-------------|
| **Pure gRPC Engine** | Engine code doesn't touch Kafka |
| **Clean Separation** | Consumption logic isolated in sidecar |
| **Consul-Native Leases** | No custom lease management in engine |
| **Health-Linked Sessions** | Stuck sidecar = dead session = released leases |
| **Independent Scaling** | gRPC load vs Kafka lag are separate concerns |
| **Replay Capability** | Kafka edges retain full replay ability |
| **Simplified Testing** | Engine can be tested with pure gRPC, no Kafka needed |

## Container Resources

| Container | CPU | Memory | Notes |
|-----------|-----|--------|-------|
| **Consul Agent** | 0.1 | 128MB | Lightweight |
| **Kafka Sidecar** | 0.5 | 2-3GB | Kafka buffers, ~50MB per topic |
| **Engine** | 1-2 | 1-2GB | Processing, gRPC, module calls |

Total per task: ~2-3 CPU, ~4-6GB RAM
