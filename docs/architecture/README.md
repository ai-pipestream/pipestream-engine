# PipeStream Engine Architecture

> The orchestration brain of the PipeStream platform

## Documentation Index

| Document | Description |
|----------|-------------|
| [Overview](./01-overview.md) | High-level architecture and design principles |
| [Processing Loop](./02-processing-loop.md) | Core engine processing flow |
| [Graph Management](./03-graph-management.md) | DAG storage, versioning, and caching |
| [Transport & Routing](./04-transport-routing.md) | gRPC/Kafka transport and CEL routing |
| [Hydration Model](./05-hydration-model.md) | Two-level document hydration |
| [Mapping & Filtering](./06-mapping-filtering.md) | In-engine transformations with CEL |
| [Module Integration](./07-module-integration.md) | How engine calls remote modules |
| [Kafka Sidecar](./08-kafka-sidecar.md) | Kafka consumption via sidecar pattern |
| [Scaling Model](./09-scaling-model.md) | Engine and module scaling patterns |
| [DLQ Handling](./10-dlq-handling.md) | Dead letter queue design |

## Quick Reference

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ARCHITECTURE                                           │
│                                                                                  │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                       │
│   │   Intake    │────►│   Engine    │────►│   Module    │                       │
│   │   (gRPC)    │     │ (Pure gRPC) │     │   (gRPC)    │                       │
│   └─────────────┘     └─────────────┘     └─────────────┘                       │
│                              ▲                                                   │
│                              │ localhost gRPC                                    │
│                       ┌──────┴──────┐                                           │
│                       │   Sidecar   │◄──── Kafka                                │
│                       │  (Consume,  │                                           │
│                       │   Hydrate)  │◄──── Consul Leases                        │
│                       └─────────────┘                                           │
│                                                                                  │
│   Key Principle: Engine is PURE gRPC                                            │
│   - Doesn't know Kafka exists                                                   │
│   - Sidecar handles all Kafka complexity                                        │
│   - Consul manages topic lease distribution                                     │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Key Principles

1. **Engine = Pure gRPC** - No Kafka awareness, just processes requests
2. **Sidecar = Kafka Bridge** - Consumes, hydrates, delivers to engine
3. **Consul = Lease Manager** - Distributes topic ownership via sessions
4. **One Transport Per Edge** - gRPC (fast) OR Kafka (async/replay), never both
5. **Two-Level Hydration** - PipeStream→PipeDoc and PipeDoc→Blob are independent
6. **Modules = Dumb Transformers** - PipeDoc in → PipeDoc out, stateless

## Edge Transport Selection

| Transport | Flow | Use Case |
|-----------|------|----------|
| **gRPC** | Engine → Engine (direct) | Fast path, low latency |
| **Kafka** | Engine → Repo → Kafka → Sidecar → Engine | Async, replayable, durable |

The frontend/graph designer chooses transport per edge based on requirements.
