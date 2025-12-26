# Pipestream Engine Overview

## What is the Pipestream Engine?

The PipeStream Engine is the central orchestration service that routes documents through processing pipelines. It is a **pure gRPC service** that:

- Receives documents from Intake, other engines, or Kafka sidecars
- Determines which processing modules to invoke
- Manages the flow through the pipeline graph
- Handles failures and dead-letter queuing

## Design Principles

### 1. Engine is Pure gRPC

The engine doesn't know Kafka exists. All inputs arrive via gRPC:

```mermaid
graph LR
    Intake[Intake] -- gRPC --> Engine[ProcessNodeRequest]
    OtherEngine[Other Engine] -- gRPC --> Engine
    Sidecar[Kafka Sidecar] -- gRPC --> Engine
    
    subgraph EngineSees [Engine Sees]
        Engine
    end
```

### 2. Sidecar Handles Kafka Complexity

```mermaid
graph LR
    subgraph ComputeTask [Compute Task]
        Consul["Consul Agent<br/>• Health check<br/>• Service reg"]
        Sidecar["Kafka Sidecar<br/>• Lease mgmt<br/>• Consume<br/>• Hydrate S3<br/>• Commit offset"]
        Engine["Engine<br/>• Pure gRPC<br/>• Stateless<br/>• Routes/maps<br/>• Calls modules"]

        Sidecar <--> Consul
        Sidecar -- gRPC --> Engine
        Engine -- Commit --> Sidecar
    end
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

```mermaid
graph TD
    subgraph PhysicalReality [Physical Reality]
        CT1[Compute Task 1]
        CT2[Compute Task 2]
        CTN[Compute Task N]
        CT1 --- Sidecar1[Sidecar]
        CT1 --- Engine1[Engine]
        CT2 --- Sidecar2[Sidecar]
        CT2 --- Engine2[Engine]
        CTN --- SidecarN[Sidecar]
        CTN --- EngineN[Engine]
    end

    subgraph LogicalView [Logical View]
        Parse --> Chunk --> Embed
        Embed --> Sink
    end

%% Node IDs are UUIDs like:
%% parser-abc-123-def
%% chunker-xyz-789-ghi
```

### 5. One Transport Per Edge, Multiple Edges per Node

| Transport | Use Case | Characteristics |
|-----------|----------|------------------|
| **gRPC** | Fast path, same cluster | Direct, low latency, no replay |
| **Kafka** | Async, cross-cluster, replay needed | Buffered, durable, replayable |

No mixing - each edge chooses one transport.

## Processing Flow

### Overview
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

### Visual Overview

```mermaid
flowchart TD
    Start([Receive PipeStream gRPC]) --> Hydrate1["Hydrate Level 1: document_ref -> PipeDoc"]
    Hydrate1 --> Filter{Filter CEL}
    Filter -- false --> End([End])
    Filter -- true --> PreMap[Pre-mapping CEL transforms]
    PreMap --> Hydrate2["Hydrate Level 2: storage_ref -> bytes"]
    Hydrate2 --> CallModule[Call Module gRPC]
    CallModule --> PostMap[Post-mapping CEL transforms]
    PostMap --> Edges{For each edge}
    
    Edges --> gRPC[gRPC edge]
    Edges --> Kafka[Kafka edge]
    
    gRPC --> CallNext[Call next engine directly]
    Kafka --> Persist[Persist to Repo & publish to Kafka]
    
    CallNext --> End
    Persist --> End
```

## Hydration Responsibilities

Both the Engine and Kafka Sidecar can perform hydration, but they have **independent strategies** and both access S3 through the **Repo Service** (never directly):

```mermaid
graph TD
    subgraph KafkaSidecar [Kafka Sidecar Hydration]
        KS1[Receive Kafka message with document_ref]
        KS2[Call Repo Service]
        KS3[Get PipeDoc from S3]
        KS4[Deliver hydrated PipeStream to Engine via gRPC]
        KS1 --> KS2 --> KS3 --> KS4
    end
    
    subgraph EngineHydration [Engine Hydration]
        E1[Receive PipeStream via gRPC]
        E2{Has document_ref?}
        E3[Call Repo Service]
        E4[Get PipeDoc from S3]
        E5{Module needs blob?}
        E6[Call Repo Service for blob]
        E7[Get blob bytes from S3]
        E1 --> E2
        E2 -- yes --> E3 --> E4 --> E5
        E2 -- no --> E5
        E5 -- yes --> E6 --> E7
    end
    
    RepoService[(Repo Service)]
    S3[(S3)]
    
    KS2 --> RepoService
    E3 --> RepoService
    E6 --> RepoService
    RepoService --> S3
```

| Component | Hydration Responsibility | When |
|-----------|-------------------------|------|
| **Kafka Sidecar** | Level 1: `document_ref` → `PipeDoc` | Always (Kafka messages only contain refs) |
| **Engine** | Level 1: `document_ref` → `PipeDoc` | When gRPC sender used a ref (rare, for >2GB docs) |
| **Engine** | Level 2: `storage_ref` → blob bytes | When module needs blob content (parsers) |

**Key points:**
- Kafka Sidecar handles its own hydration before calling Engine
- Engine handles hydration for gRPC-delivered documents when needed
- Both use **Repo Service gRPC API** - never access S3 directly
- For now, gRPC path rarely needs Level 1 hydration (2GB limit is generous)
- Level 2 blob hydration is always Engine's responsibility

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
| **Direct S3 Access** | Never - always through Repo Service |
