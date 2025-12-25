# Engine Overview

## What is the Engine?

The PipeStream Engine is the central orchestration service that routes documents through processing pipelines. It is the "brain" that:

- Receives documents from Intake or other engines
- Determines which processing modules to invoke
- Manages the flow through the pipeline graph
- Handles failures and dead-letter queuing

## Design Principles

### 1. Engine is the Orchestrator, Not a Processor

The engine doesn't transform documents itself. It orchestrates:

```
┌─────────────────────────────────────────────────────────────────┐
│                         ENGINE                                   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │               IN-ENGINE (No Network Hops)                │   │
│  │                                                          │   │
│  │  1. Receive PipeStream                                   │   │
│  │  2. Rehydrate PipeDoc from S3 (Repo call)               │   │
│  │  3. Filter (CEL)                                         │   │
│  │  4. Pre-mapping (CEL transforms)                         │   │
│  │  5. Call Module ──────────────────► Only network hop     │   │
│  │  6. Post-mapping (CEL transforms)                        │   │
│  │  7. Resolve next edges (CEL conditions)                  │   │
│  │  8. Route to next node                                   │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 2. Modules are Stateless Transformers

Modules know nothing about:
- The graph topology
- Other nodes in the pipeline
- Routing decisions
- Where they are in the pipeline

They simply: `PipeDoc in → transform → PipeDoc out`

### 3. Nodes are Logical Constructs

```
Physical Reality:               Logical View (Graph):

┌─────────────────┐            ┌───────┐    ┌───────┐    ┌───────┐
│  Engine Pod 1   │            │ Parse │───▶│ Chunk │───▶│ Embed │
├─────────────────┤            └───────┘    └───────┘    └───────┘
│  Engine Pod 2   │                              │
├─────────────────┤                              ▼
│  Engine Pod 3   │                         ┌───────┐
└─────────────────┘                         │ Sink  │
        │                                   └───────┘
        ▼
   All pods can process
   ANY node - they share
   the same graph cache
```

### 4. Graph is Stored as Versioned JSON

- Full graph snapshot stored in Postgres JSONB
- Each update creates a new version (no diffs)
- Engine caches active version in-memory
- Updates streamed via Kafka topic

### 5. Transport is an Edge Property

| Transport | Use Case | Behavior |
|-----------|----------|----------|
| **Kafka (MESSAGING)** | Async, batch, cost-sensitive | Buffered, rewindable, S3 offload |
| **gRPC** | Real-time, latency-sensitive | Direct call, inline payload |

## Component Interactions

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Intake  │────▶│  Engine  │────▶│  Module  │     │   Repo   │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
                      │                                  ▲
                      │          ┌──────────┐           │
                      └─────────▶│  Kafka   │           │
                      │          └──────────┘           │
                      │                                  │
                      └──────────────────────────────────┘
                              Hydrate/Offload PipeDoc
```

## What's Embedded in Engine

The following services run in the same JVM as the engine:

| Service | Purpose |
|---------|---------|
| **MappingService** | Apply field transformations (CEL-based) |
| **Graph Cache** | In-memory graph with helper lookups |
| **CEL Evaluator** | Compile and evaluate CEL expressions |
| **Kafka Consumer Manager** | Dynamic topic subscription |
