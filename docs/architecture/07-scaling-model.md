# Scaling Model

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│   ENGINE (Always On)                    MODULES (Scale to Zero)             │
│   ──────────────────                    ─────────────────────               │
│                                                                              │
│   ┌─────────────────┐                   ┌─────────────────┐                 │
│   │  Engine Pool    │                   │  Tika Parser    │                 │
│   │                 │    gRPC call      │  (0 → N pods)   │                 │
│   │  • Graph cache  │ ───────────────▶  │                 │                 │
│   │  • Routing      │                   │  Scales on      │                 │
│   │  • Mapping      │                   │  demand         │                 │
│   │  • Orchestration│                   └─────────────────┘                 │
│   │                 │                                                        │
│   │  Baseline: 2-3  │                   ┌─────────────────┐                 │
│   │  pods always    │                   │  Embedder (GPU) │                 │
│   │                 │ ───────────────▶  │  (0 → N pods)   │                 │
│   └─────────────────┘                   │                 │                 │
│                                         │  $$$$ when on   │                 │
│                                         │  $0 when idle   │                 │
│                                         └─────────────────┘                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Cost Model

| Component | Scaling | Cost When Idle |
|-----------|---------|----------------|
| **Engine** | Baseline + HPA | Minimal (2-3 small pods) |
| **Kafka** | Always on | Fixed (managed service) |
| **Tika Parser** | 0 → N | Zero |
| **Embedder (GPU)** | 0 → N | Zero |
| **Chunker** | 0 → N | Zero |
| **OpenSearch Sink** | 0 → N | Zero |

## Why This Works

### Engine: Always-On Orchestrator

- Lightweight (CPU only, no GPU)
- Stateless (graph in cache, docs in S3)
- Handles any node (logical, not physical mapping)
- Cheap to run baseline

### Modules: Scale-to-Zero Processors

- Each module is an independent gRPC service
- Scales based on demand (KEDA + Kafka lag)
- Expensive resources (GPUs) only used when needed
- Cold start handled by Kafka buffering

## KEDA Integration

```yaml
# KEDA ScaledObject for Tika Parser
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: tika-parser-scaler
spec:
  scaleTargetRef:
    name: tika-parser
  minReplicaCount: 0              # Scale to zero!
  maxReplicaCount: 20
  cooldownPeriod: 300             # 5 min before scale down
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka:9092
        topic: prod.tika-parser   # Node's input topic
        consumerGroup: tika-parser-cg
        lagThreshold: "10"        # Scale up when lag > 10
```

```yaml
# KEDA ScaledObject for GPU Embedder
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: embedder-scaler
spec:
  scaleTargetRef:
    name: embedder
  minReplicaCount: 0
  maxReplicaCount: 5              # Fewer due to GPU cost
  cooldownPeriod: 600             # 10 min (GPUs expensive)
  triggers:
    - type: kafka
      metadata:
        topic: prod.embedder
        lagThreshold: "5"         # Aggressive scaling
```

## Batch Ingestion Scenario

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  TIME      EVENT                                    MODULE PODS              │
│─────────────────────────────────────────────────────────────────────────────│
│  T+0       Intake receives 10,000 docs              Parser: 0               │
│            └─▶ Publishes to Kafka                   Chunker: 0              │
│                                                     Embedder: 0             │
│─────────────────────────────────────────────────────────────────────────────│
│  T+10s     Engine consuming, calls Parser           Parser: 0 → 3           │
│            KEDA sees lag on parser topic            Chunker: 0              │
│                                                     Embedder: 0             │
│─────────────────────────────────────────────────────────────────────────────│
│  T+60s     Parser processing, output to Chunker    Parser: 3 → 5           │
│            KEDA sees lag on chunker topic           Chunker: 0 → 3          │
│                                                     Embedder: 0             │
│─────────────────────────────────────────────────────────────────────────────│
│  T+120s    Chunker processing, output to Embedder   Parser: 5               │
│            KEDA sees lag on embedder topic          Chunker: 3              │
│                                                     Embedder: 0 → 2 (GPU)   │
│─────────────────────────────────────────────────────────────────────────────│
│  T+300s    Pipeline at full throughput              Parser: 5               │
│                                                     Chunker: 3              │
│                                                     Embedder: 2             │
│─────────────────────────────────────────────────────────────────────────────│
│  T+600s    Batch complete, lag = 0                  Parser: 5 → 0           │
│            Cooldown begins                          Chunker: 3 → 0          │
│                                                     Embedder: 2 → 0         │
│─────────────────────────────────────────────────────────────────────────────│
│  T+900s    All idle                                 Parser: 0               │
│                                                     Chunker: 0              │
│                                                     Embedder: 0             │
└─────────────────────────────────────────────────────────────────────────────┘

Cost: Only pay for actual processing time!
```

## Cold Start Handling

### Kafka Path (Async Edges)

```
Engine needs Parser, but Parser scaled to zero:

1. Engine publishes to parser topic
2. Kafka buffers message
3. KEDA sees lag > threshold
4. KEDA scales Parser 0 → 1
5. Parser starts, registers with Consul
6. Parser consumes from topic
7. Processing continues

Latency: ~30-60 seconds (acceptable for batch)
```

### gRPC Path (Sync Edges)

```
Engine needs Parser via gRPC, but Parser scaled to zero:

1. Engine checks Consul - no healthy instances
2. Options:
   a. Convert to Kafka path (if configured)
   b. Retry with backoff (wait for scale up)
   c. Route to DLQ (if timeout exceeded)

For real-time paths, set minReplicaCount: 1
```

## Real-Time vs Batch Configuration

| Path Type | Module minReplicas | Edge Transport | Cold Start |
|-----------|-------------------|----------------|------------|
| **Real-time** | 1+ | gRPC | None (always warm) |
| **Batch** | 0 | Kafka | Acceptable (buffered) |
| **Hybrid** | 0 | Kafka | Acceptable |

## Resource Efficiency Comparison

```
TRADITIONAL (Physical Nodes):       PIPESTREAM (Logical Nodes):

┌─────────┐ ┌─────────┐ ┌─────────┐   ┌─────────────────────────────┐
│ Parser  │ │ Chunker │ │Embedder │   │      Engine Pool            │
│ Pod x3  │ │ Pod x2  │ │ Pod x5  │   │                             │
└─────────┘ └─────────┘ └─────────┘   │  Any pod handles any node   │
     │           │           │         │  Scale based on TOTAL load  │
     ▼           ▼           ▼         │  Modules scale independently│
  Idle when   Idle when   Idle when    └─────────────────────────────┘
  no PDFs     no chunks   no embeds                  │
                                                     ▼
  Waste: Provisioned for peak         Waste: Near zero
         per-node                      - Engine: minimal baseline
                                       - Modules: 0 when idle
```

## Knative Alternative

If using Knative instead of KEDA:

```yaml
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: tika-parser
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/min-scale: "0"
        autoscaling.knative.dev/max-scale: "20"
    spec:
      containers:
        - image: pipestream/tika-parser:latest
          ports:
            - containerPort: 9090
              name: h2c  # gRPC
```

Knative handles:
- Scale to zero
- Request-based autoscaling
- Cold start with queue-proxy buffering
