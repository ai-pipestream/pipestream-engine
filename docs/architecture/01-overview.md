# Engine Overview

## What is the Engine?

The PipeStream Engine is the central orchestration service that routes documents through processing pipelines. It is a **pure gRPC service** that:

- Receives documents from Intake, other engines, or Kafka sidecars
- Determines which processing modules to invoke
- Manages the flow through the pipeline graph
- Handles failures and dead-letter queuing

## Design Principles

### 1. Engine is Pure gRPC

The engine doesn't know Kafka exists. All inputs arrive via gRPC:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ENGINE INPUTS                                          │
│                                                                                  │
│   Source              Transport       Engine Sees                               │
│   ──────              ─────────       ───────────                               │
│   Intake         ───► gRPC       ───► ProcessNodeRequest                        │
│   Other Engine   ───► gRPC       ───► ProcessNodeRequest                        │
│   Kafka Sidecar  ───► gRPC       ───► ProcessNodeRequest (same!)                │
│                                                                                  │
│   Engine code is identical regardless of source                                 │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2. Sidecar Handles Kafka Complexity

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     COMPUTE TASK                                                 │
│                                                                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │  Consul Agent   │    │  Kafka Sidecar  │    │     Engine      │             │
│  │                 │    │                 │    │                 │             │
│  │  • Health check │◄───│  • Lease mgmt   │    │  • Pure gRPC    │             │
│  │  • Service reg  │    │  • Consume      │───►│  • Stateless    │             │
│  │                 │    │  • Hydrate S3   │    │  • Routes/maps  │             │
│  │                 │    │  • Commit offset│◄───│  • Calls modules│             │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘             │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3. Modules are Stateless Transformers

Modules know nothing about:
- The graph topology
- Other nodes in the pipeline
- Routing decisions
- Where they are in the pipeline
- Kafka, S3, or any infrastructure

They simply: `PipeDoc in → transform → PipeDoc out`

### 4. Nodes are Logical Constructs

```
Physical Reality:                   Logical View (Graph):

┌─────────────────────┐             ┌───────┐    ┌───────┐    ┌───────┐
│  Compute Task 1     │             │ Parse │───►│ Chunk │───►│ Embed │
│  ├── Sidecar        │             └───────┘    └───────┘    └───────┘
│  └── Engine         │                                │
├─────────────────────┤                                ▼
│  Compute Task 2     │                          ┌───────┐
│  ├── Sidecar        │                          │ Sink  │
│  └── Engine         │                          └───────┘
├─────────────────────┤
│  Compute Task N     │             Node IDs are UUIDs like:
│  ├── Sidecar        │               parser-abc-123-def
│  └── Engine         │               chunker-xyz-789-ghi
└─────────────────────┘
```

### 5. One Transport Per Edge

| Transport | Use Case | Characteristics |
|-----------|----------|------------------|
| **gRPC** | Fast path, same cluster | Direct, low latency, no replay |
| **Kafka** | Async, cross-cluster, replay needed | Buffered, durable, replayable |

No mixing - each edge chooses one transport.

## Processing Flow

```
1. Receive PipeStream (gRPC - from any source)
2. Hydrate Level 1: document_ref → PipeDoc (if needed)
3. Filter (CEL) - skip node if false
4. Pre-mapping (CEL transforms)
5. Hydrate Level 2: blob storage_ref → bytes (if parser module needs it)
6. Call Module (gRPC)
7. Post-mapping (CEL transforms)
8. Determine outgoing edges (CEL conditions)
9. For each edge:
   - gRPC edge: call next engine directly
   - Kafka edge: persist to Repo, publish to Kafka
```

## What's Embedded in Engine

| Component | Purpose |
|-----------|---------|
| **MappingService** | Apply field transformations (CEL-based) |
| **Graph Cache** | In-memory graph with helper lookups |
| **CEL Evaluator** | Compile and evaluate CEL expressions |
| **Module Caller** | gRPC client pool for modules |
| **Repo Client** | gRPC client for hydration/persistence |

## What's NOT in Engine

| Component | Where It Lives |
|-----------|----------------|
| **Kafka Consumer** | Kafka Sidecar (separate container) |
| **Topic Leases** | Consul (via sidecar) |
| **Document Storage** | Repo Service + S3 |
| **Module Logic** | Remote Module Services |
