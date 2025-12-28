# Kafka Sidecar Requirements Document

## Overview

The Kafka Sidecar is a specialized service that bridges asynchronous Kafka messaging with the synchronous Pipestream Engine. It isolates Kafka consumption complexity from the Engine, allowing the Engine to remain a pure gRPC service focused on orchestration and processing.

**Repository**: [pipestream-engine-kafka-sidecar](https://github.com/ai-pipestream/pipestream-engine-kafka-sidecar)

**Related Documentation**:
- [Engine Architecture: Kafka Sidecar Pattern](../../docs/architecture/08-kafka-sidecar.md)
- [Engine Architecture: Hydration Model](../../docs/architecture/05-hydration-model.md)
- [Engine Architecture: DLQ Handling](../../docs/architecture/10-dlq-handling.md)
- [Engine Architecture: Transport Routing](../../docs/architecture/04-transport-routing.md)
- [Repository Service Design](../../../repository-service/DESIGN.md)

## Core Responsibilities

1. **Kafka Consumption**: Consume messages from intake and node topics
2. **Lease Management**: Acquire and maintain topic leases via Consul
3. **Level 1 Hydration**: Resolve `DocumentReference` to full `PipeDoc` via Repository Service
4. **gRPC Handoff**: Deliver hydrated documents to Engine via gRPC
5. **DLQ Publishing**: Publish failed messages to Dead Letter Queue topics
6. **Offset Management**: Commit Kafka offsets only after successful processing

## Functional Requirements

### FR1: Topic Consumption

#### FR1.1: Intake Topics
- **Pattern**: `intake.{datasource_id}`
- **Publisher**: Repository Service
- **Engine Endpoint**: `IntakeHandoff()`
- **Purpose**: Initial document ingestion into the system
- **Message Format**: `PipeStream` containing `DocumentReference` (not full `PipeDoc`)

#### FR1.2: Node Topics
- **Pattern**: `pipestream.{cluster}.{node_id}`
- **Publisher**: Engine (via Repository Service)
- **Engine Endpoint**: `ProcessNode()`
- **Purpose**: Inter-node routing for async pipeline steps
- **Message Format**: `PipeStream` containing `DocumentReference` (not full `PipeDoc`)

#### FR1.3: Message Schema
- **Key**: UUID (string representation) for deterministic partitioning
- **Value**: `PipeStream` protobuf message
- **Headers**: Optional metadata (account_id, request_id, etc.)

### FR2: Lease Management via Consul

#### FR2.1: Service Registration
- Register a Consul session upon startup
- Maintain session health (heartbeat)
- Release all leases on graceful shutdown

#### FR2.2: Topic Discovery
- Poll Consul KV store for available topics:
  - `pipestream/intake-topics/{datasource_id}`
  - `pipestream/node-topics/{cluster_id}.{node_id}`
- Support dynamic topic addition/removal (polling interval configurable)

#### FR2.3: Lock Acquisition
- Attempt to acquire ephemeral KV lock for each discovered topic
- Lock key format: `pipestream/locks/{topic_name}`
- Lock linked to Consul session (auto-released on session expiry)
- Support maximum leases per sidecar (configurable, default: 50)

#### FR2.4: Lease Persistence
- Maintain lock as long as Consul session is active
- Automatically release lock on session expiry (sidecar failure)
- Re-acquire leases on session renewal

### FR3: Level 1 Hydration

#### FR3.1: DocumentReference Resolution
- Extract `DocumentReference` from `PipeStream`
- Call Repository Service: `GetPipeDocByReference()`
- Parameters:
  - `doc_id` (from `DocumentReference`)
  - `graph_location_id` (from `DocumentReference.source_node_id`)
  - `account_id` (from `DocumentReference.account_id`)

#### FR3.2: Hydrated Stream Construction
- Replace `DocumentReference` with full `PipeDoc` in `PipeStream`
- Preserve all `PipeStream` metadata (hop_count, processing_path, etc.)
- Handle hydration failures (retry with backoff, then DLQ)

#### FR3.3: Repository Service Client
- Use reactive gRPC client (Mutiny) for Repository Service
- Support service discovery (Consul or static configuration)
- Handle connection failures and retries

### FR4: gRPC Handoff to Engine

#### FR4.1: Intake Handoff
- **Endpoint**: `EngineV1Service.IntakeHandoff()`
- **Request**:
  ```protobuf
  message IntakeHandoffRequest {
    PipeStream stream = 1;
    string datasource_id = 2;
    bool doc_stored_in_repo = 3;  // Always true for Kafka path
  }
  ```
- Extract `datasource_id` from topic name (`intake.{datasource_id}`)
- Set `doc_stored_in_repo = true` (document already persisted)

#### FR4.2: Process Node
- **Endpoint**: `EngineV1Service.ProcessNode()`
- **Request**:
  ```protobuf
  message ProcessNodeRequest {
    PipeStream stream = 1;
  }
  ```
- Extract target node from topic name or `PipeStream.current_node_id`

#### FR4.3: Engine Client
- Use reactive gRPC client (Mutiny) for Engine
- Default to `localhost` (sidecar runs alongside Engine)
- Support configurable Engine address for testing/debugging
- Handle Engine unavailability (retry with backoff, then DLQ)

### FR5: Dead Letter Queue (DLQ) Publishing

#### FR5.1: DLQ Topic Naming
- **Pattern**: `dlq.{cluster_id}.{node_id}`
- Extract cluster and node from original topic or `PipeStream`
- Create DLQ topic if it doesn't exist (via Kafka admin client)

#### FR5.2: DLQ Message Schema
```protobuf
message DlqMessage {
  PipeStream stream = 1;                    // Dehydrated (DocumentReference only)
  string error_type = 2;                    // "TIMEOUT", "CONNECTION_REFUSED", etc.
  string error_message = 3;
  google.protobuf.Timestamp failed_at = 4;
  int32 retry_count = 5;
  string failed_node_id = 6;
  string original_topic = 7;               // For replay context
  int32 original_partition = 8;
  int64 original_offset = 9;
}
```

#### FR5.3: DLQ Publishing Triggers
- Engine returns `ProcessNodeResponse.success = false` after retries exhausted
- Engine unreachable (connection refused, timeout)
- Hydration failure after retries exhausted
- Invalid message format (malformed protobuf)

#### FR5.4: Offset Commitment After DLQ
- Commit offset **after** successful DLQ publish
- Ensures poison messages don't block pipeline
- Log DLQ publish for monitoring/alerting

### FR6: Offset Management

#### FR6.1: Manual Offset Commits
- Commit offsets **only** after successful Engine processing
- Use `commitSync()` for at-least-once delivery guarantee
- Commit per message (not batch) for precise progress tracking

#### FR6.2: Offset Storage
- Use Kafka's built-in offset storage (consumer group)
- Consumer group ID: `pipestream-sidecar` (configurable)
- Support offset reset policies (earliest, latest, none)

#### FR6.3: Failure Handling
- **Before commit**: Message will be re-processed by next sidecar to acquire lease
- **After commit**: Message is considered processed (even if Engine failed)
  - Failed messages go to DLQ before commit
  - Ensures no message loss

### FR7: Retry Logic

#### FR7.1: Retry Configuration
- Maximum retries: 3 (configurable per topic/node)
- Backoff strategy: Exponential (configurable)
- Retryable errors:
  - Engine unavailable (connection refused, timeout)
  - Repository Service unavailable
  - Transient network errors

#### FR7.2: Retry Tracking
- Track retry count per message (in-memory or via message headers)
- Log retry attempts for monitoring
- Exhaust retries before DLQ publishing

#### FR7.3: Non-Retryable Errors
- Invalid message format (malformed protobuf)
- Logical failures (Engine returns success=false with logical error)
  - These are valid processing outcomes, not infrastructure failures

## Non-Functional Requirements

### NFR1: Performance
- **Latency**: < 100ms p99 for hydration + handoff (excluding Engine processing time)
- **Throughput**: Support 1000+ messages/second per sidecar
- **Resource Usage**:
  - CPU: 0.5 cores (configurable)
  - Memory: 2-3GB (configurable, ~50MB per active topic lease)

### NFR2: Reliability
- **Availability**: 99.9% uptime (excluding planned maintenance)
- **Fault Tolerance**: Automatic lease release on failure (via Consul session expiry)
- **Message Guarantee**: At-least-once delivery (via manual offset commits)

### NFR3: Observability
- **Metrics**:
  - Messages consumed per topic
  - Hydration latency (p50, p95, p99)
  - Engine handoff latency
  - DLQ publish count
  - Retry count histogram
  - Active lease count
  - Offset lag per topic
- **Logging**:
  - Structured logging (JSON format)
  - Log level: INFO (default), DEBUG (configurable)
  - Include: topic, partition, offset, doc_id, account_id, request_id
- **Tracing**: Support distributed tracing (OpenTelemetry/Jaeger)

### NFR4: Scalability
- **Horizontal Scaling**: Multiple sidecars can run in parallel
- **Lease Distribution**: Automatic load balancing via Consul locks
- **Topic Limits**: Support 1000+ topics per cluster (via lease limits per sidecar)

### NFR5: Configuration
- **Environment Variables**: All configuration via environment variables
- **Configuration File**: Optional YAML/Properties file support
- **Hot Reload**: Support configuration reload without restart (where applicable)

## Technical Requirements

### TR1: Technology Stack
- **Language**: Java (Quarkus framework)
- **gRPC**: Mutiny-based reactive gRPC clients
- **Kafka**: SmallRye Reactive Messaging (Quarkus Kafka extension)
- **Consul**: Consul Java client (for service discovery and KV operations)
- **Protobuf**: Generated code from `pipestream-protos` repository

### TR2: Dependencies
- Repository Service gRPC client
- Engine gRPC client
- Kafka client (via Quarkus)
- Consul client
- Protobuf runtime

### TR3: Service Discovery
- **Repository Service**: Via Consul service discovery or static configuration
- **Engine**: Default `localhost:9090` (configurable)
- **Consul**: Via Consul agent (typically localhost:8500)

### TR4: Error Handling
- **Graceful Degradation**: Continue processing other topics if one fails
- **Circuit Breaker**: Implement circuit breaker for Engine/Repo Service calls
- **Timeout Configuration**: Configurable timeouts for all external calls

## Implementation Phases

### Phase 1: Core Consumption (MVP)
- [ ] Consul session management
- [ ] Topic discovery and lease acquisition
- [ ] Kafka consumer setup (intake topics only)
- [ ] Level 1 hydration
- [ ] Engine gRPC handoff (IntakeHandoff)
- [ ] Offset management
- [ ] Basic error handling

### Phase 2: Node Topics & DLQ
- [ ] Node topic consumption
- [ ] ProcessNode gRPC handoff
- [ ] DLQ publishing
- [ ] Retry logic with backoff
- [ ] Enhanced error handling

### Phase 3: Observability & Hardening
- [ ] Metrics collection (Prometheus)
- [ ] Distributed tracing
- [ ] Health checks
- [ ] Circuit breakers
- [ ] Configuration management
- [ ] Documentation

### Phase 4: Advanced Features
- [ ] Batch processing (configurable)
- [ ] Rate limiting
- [ ] Priority queues
- [ ] Custom retry policies per topic

## Testing Requirements

### Unit Tests
- Lease acquisition logic
- Hydration logic
- DLQ message construction
- Error handling scenarios

### Integration Tests
- End-to-end flow: Kafka → Hydration → Engine → Commit
- Lease management with Consul
- DLQ publishing
- Failure scenarios (Engine down, Repo Service down)

### Performance Tests
- Throughput benchmarks
- Latency measurements
- Resource usage under load

## Deployment Requirements

### Container
- Docker image based on Quarkus JVM base image
- Multi-arch support (amd64, arm64)
- Health check endpoint (`/q/health`)

### Kubernetes
- Deployment manifest
- Service account and RBAC (if needed)
- ConfigMap for configuration
- Resource limits (CPU, memory)

### Configuration
- Environment variables for all settings
- Support for ConfigMap/Secrets in Kubernetes
- Default values for development

## Security Requirements

### SR1: Authentication
- gRPC: Support TLS/mTLS for Engine and Repository Service
- Kafka: Support SASL/SCRAM authentication
- Consul: Support ACL tokens

### SR2: Authorization
- Repository Service: Account isolation (enforced by Repo Service)
- Engine: No authorization (handled by Engine)

### SR3: Secrets Management
- Support Kubernetes Secrets
- Support external secret management (Vault, AWS Secrets Manager)

## References

1. [Engine Architecture: Kafka Sidecar Pattern](../../docs/architecture/08-kafka-sidecar.md)
2. [Engine Architecture: Hydration Model](../../docs/architecture/05-hydration-model.md)
3. [Engine Architecture: DLQ Handling](../../docs/architecture/10-dlq-handling.md)
4. [Engine Architecture: Transport Routing](../../docs/architecture/04-transport-routing.md)
5. [Repository Service Design](../../../repository-service/DESIGN.md)
6. [Repository Service: S3 Path Structure](../../../repository-service/docs/S3-PATH-STRUCTURE.md)

## Open Questions

1. Should sidecar support batch processing (multiple messages before commit)?
2. Should sidecar support priority queues (process high-priority topics first)?
3. Should sidecar support custom retry policies per topic/node?
4. Should sidecar expose a management API for lease control?
5. Should sidecar support message filtering (skip certain messages)?


