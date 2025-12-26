# Scaling Model

## Overview

Engines are **always busy** handling gRPC requests. They are not primarily Kafka consumers waiting for messages.

```
┌─────────────────────────────────────────────────────────────────────┐
│                     ENGINE WORKLOAD                                  │
│                                                                      │
│  Primary (always active):                                           │
│  ├── Inbound gRPC from Intake                                       │
│  ├── Inbound gRPC from other Engines                                │
│  ├── Outbound gRPC to Modules                                       │
│  ├── Outbound gRPC to Repo (hydration/persistence)                  │
│  └── Outbound gRPC to other Engines                                 │
│                                                                      │
│  Secondary (specific edges only):                                   │
│  └── Kafka consumption (when edges use Kafka transport)             │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Engine Scaling

Scale engines based on **gRPC load**, not Kafka partition count:

| Metric | Scale Trigger |
|--------|---------------|
| CPU utilization | > 70% |
| gRPC request latency p99 | > 500ms |
| gRPC request queue depth | > 100 |
| Memory utilization | > 80% |

```yaml
# HPA for engines
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: pipestream-engine
spec:
  minReplicas: 3
  maxReplicas: 50
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

## Module Scaling (Scale to Zero)

Modules are different - they're called by engines and can scale to zero:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     MODULE SCALING                                   │
│                                                                      │
│  Modules are remote gRPC services:                                  │
│  ├── Tika Parser (0 → N pods)                                       │
│  ├── Docling Parser (0 → N pods)                                    │
│  ├── Chunker (0 → N pods)                                           │
│  ├── Embedder (0 → N pods, GPU expensive!)                          │
│  └── Sinks (0 → N pods)                                             │
│                                                                      │
│  Scale based on:                                                    │
│  ├── gRPC request rate from engines                                 │
│  └── Kafka consumer lag (if module consumes directly)               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

```yaml
# KEDA for GPU embedder
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: embedder-scaler
spec:
  scaleTargetRef:
    name: embedder
  minReplicaCount: 0              # Scale to zero!
  maxReplicaCount: 10
  cooldownPeriod: 300             # 5 min before scale down
  triggers:
    - type: prometheus
      metadata:
        query: sum(rate(grpc_server_started_total{service="embedder"}[1m]))
        threshold: "10"
```

## Cost Model

| Component | Scaling | Cost When Idle |
|-----------|---------|----------------|
| **Engine** | Always on (min 3) | Baseline cost |
| **Kafka** | Always on | Fixed (managed service) |
| **Parser modules** | 0 → N | Zero when no docs |
| **Embedder (GPU)** | 0 → N | Zero when idle |
| **Repo Service** | Always on (min 2) | Baseline cost |

## Kafka Consumer Coordination

See [Consumer Coordination](./08-consumer-coordination.md) for details on managing Kafka partition assignments when multiple engines need to consume from Kafka edges.

**Key insight:** Even if an engine has no Kafka partitions assigned, it's still busy handling gRPC work.

## Batch Ingestion Pattern

```
┌─────────────────────────────────────────────────────────────────────┐
│  BATCH INGESTION (10,000 documents)                                  │
│                                                                      │
│  T+0:   Intake receives batch                                       │
│         └── Sends to Engine via gRPC (or Kafka if huge batch)       │
│                                                                      │
│  T+1:   Engines processing, calling Tika                            │
│         └── KEDA scales Tika 0 → 5 (based on gRPC load)             │
│                                                                      │
│  T+30:  Parsed docs flowing to Chunker                              │
│         └── KEDA scales Chunker 0 → 3                               │
│                                                                      │
│  T+60:  Chunks flowing to Embedder                                  │
│         └── KEDA scales Embedder 0 → 2 (GPU)                        │
│                                                                      │
│  T+300: Batch complete                                              │
│         └── Modules scale back to 0                                 │
│         └── Engines continue handling other work                    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Multi-Threaded Processing

Each engine handles multiple concurrent requests:

```java
// Quarkus gRPC service with Vert.x
@GrpcService
public class EngineService {
    
    // Each gRPC call runs on Vert.x event loop
    // Non-blocking I/O for Kafka, gRPC, S3
    
    @Override
    public Uni<ProcessNodeResponse> processNode(ProcessNodeRequest request) {
        return hydrate(request.getStream())
            .flatMap(this::applyFilter)
            .flatMap(this::callModule)
            .flatMap(this::routeToNext);
    }
}
```

For Kafka consumption, one engine can consume multiple partitions:

```java
// Multiple partitions per consumer
kafkaConsumer.assign(List.of(
    new TopicPartition("node-abc", 0),
    new TopicPartition("node-abc", 1),
    new TopicPartition("node-xyz", 0)
));
```
