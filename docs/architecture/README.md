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
| [Consumer Coordination](./08-consumer-coordination.md) | Kafka partition lease management (TBD) |
| [Scaling Model](./09-scaling-model.md) | Engine and module scaling patterns |
| [DLQ Handling](./10-dlq-handling.md) | Dead letter queue design |

## Quick Reference

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ENGINE                                       │
│                  (Orchestration Brain)                               │
│                                                                      │
│  Typical Flow: gRPC → gRPC → gRPC (fast path)                       │
│                                                                      │
│  1. Receive PipeStream (gRPC primary, Kafka for specific edges)     │
│  2. Hydrate Level 1: PipeStream → PipeDoc (if document_ref)         │
│  3. Filter (CEL) - skip node if false                               │
│  4. Pre-mapping (CEL transforms)                                    │
│  5. Hydrate Level 2: Blob content (if parser module needs it)       │
│  6. Call Module (gRPC)                                              │
│  7. Post-mapping (CEL transforms)                                   │
│  8. Persist to Repo (if Kafka edge or policy requires)              │
│  9. Route to next node(s)                                           │
│                                                                      │
│  Graph: Full JSON in Postgres, versioned, cached in-memory          │
│  Nodes: Logical construct with UUID instance IDs                    │
│  Modules: Remote gRPC services, scale 0→N independently             │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Principles

1. **gRPC is Primary** - Most hops are synchronous gRPC, Kafka for specific edges
2. **Engine = Smart Orchestrator** - Routing, mapping, hydration, filtering
3. **Module = Dumb Transformer** - PipeDoc in → PipeDoc out, stateless
4. **Two-Level Hydration** - PipeStream→PipeDoc and PipeDoc→Blob are independent
5. **Nodes = Logical Construct** - UUID instance IDs, not module names
6. **Transport = Edge Property** - Each edge declares gRPC or Kafka
