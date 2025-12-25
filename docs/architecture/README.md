# PipeStream Engine Architecture

> The orchestration brain of the PipeStream platform

## Documentation Index

| Document | Description |
|----------|-------------|
| [Overview](./01-overview.md) | High-level architecture and design principles |
| [Processing Loop](./02-processing-loop.md) | Core engine processing flow |
| [Graph Management](./03-graph-management.md) | DAG storage, versioning, and caching |
| [Transport & Routing](./04-transport-routing.md) | Kafka/gRPC transport and CEL routing |
| [Mapping & Filtering](./05-mapping-filtering.md) | In-engine transformations with CEL |
| [Module Integration](./06-module-integration.md) | How engine calls remote modules |
| [Scaling Model](./07-scaling-model.md) | Serverless modules and resource efficiency |
| [DLQ Handling](./08-dlq-handling.md) | Dead letter queue design |

## Quick Reference

```
┌─────────────────────────────────────────────────────────────────┐
│                         ENGINE                                   │
│                  (Orchestration Brain)                           │
│                                                                  │
│  • Receives PipeStream (Kafka or gRPC)                          │
│  • Rehydrates PipeDoc from S3                                   │
│  • Applies filter (CEL) - skip if false                         │
│  • Applies pre-mappings (CEL transforms)                        │
│  • Calls remote Module (gRPC)                                   │
│  • Applies post-mappings (CEL transforms)                       │
│  • Resolves next edges (CEL conditions)                         │
│  • Routes to next nodes (Kafka or gRPC per edge)                │
│                                                                  │
│  Graph: Full JSON in Postgres, versioned, cached in-memory      │
│  Nodes: Logical construct - any engine handles any node         │
│  Modules: Remote gRPC services, scale 0→N independently         │
└─────────────────────────────────────────────────────────────────┘
```

## Key Principles

1. **Engine = Smart Orchestrator** - Routing, mapping, hydration, filtering
2. **Module = Dumb Transformer** - PipeDoc in → PipeDoc out, stateless
3. **Graph = Logical DAG** - Versioned JSON in Postgres, cached in-memory
4. **Nodes = Logical Construct** - Any engine instance handles any node
5. **Transport = Edge Property** - Kafka (async) or gRPC (sync) per edge
