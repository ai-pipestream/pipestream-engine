## Overview

This document captures the agreed design for aligning document intake with the Engine-owned DAG graph, enabling end-to-end (E2E) processing while keeping ingestion efficient (no full payload buffering) and configuration UI-friendly.

Primary goals:

- Make **Engine the owner of graph routing** and execution semantics.
- Treat **datasource configuration as graph-versioned policy**, similar to modules, without pretending a datasource is a normal processing step.
- Support two ingress modes:
  - **HTTP upload**: always **store first** (repo-service → object storage) and then publish **reference-only** into the pipeline.
  - **gRPC ingestion**: allow **inline payload** for trusted/controlled clients (optionally also allow ref-only).
- Provide a structured **IngestContext** that downstream modules can use for routing/hints/policy.
- Keep **S3/object storage** as an implementation detail of **repo-service** (other services should not need to parse provider-specific URLs).

## Key Concepts

### DatasourceDefinition vs DatasourceInstance

- **DatasourceDefinition** (service-level): describes a connector/datasource type and its capabilities.
  - Owned by `connector-admin` (later: `datasource-admin`).
  - Listed in the UI so a graph author can select available datasources.

- **DatasourceInstance** (graph-level): a graph-versioned configured instance of a datasource.
  - Stored in the **graph** alongside nodes/edges.
  - Provides ingestion policy and downstream hints.
  - Binds ingress to an Engine **entry node**.

This mirrors the module model:

- ModuleDefinition (service-level) ↔ DatasourceDefinition
- Node (graph-level step) ↔ DatasourceInstance (graph-level ingress)

But DatasourceInstance is **not** executed via `ProcessData`.

### IngestContext (structured)

A structured message carried with the `PipeStream` (likely in `StreamMetadata`) containing:

- `datasource_instance_id`
- `ingress_mode` (HTTP_STAGED vs GRPC_INLINE / GRPC_STAGED)
- `policy` fields (size limits, retention hints, encryption policy references)
- `output_hints` (e.g., preferred OpenSearch index, base ACLs)
- connector-specific `custom_config` (JSON-Forms-driven) for UI and downstream behavior

This is a typed contract, not a stringly-typed map.

## Execution Semantics

### Hop history: “hop 0” ingress record

- DatasourceInstance should appear as a **history record at hop 0**.
- This record represents ingestion/staging/acceptance, not module execution.
- It should include:
  - timestamps
  - datasource instance id/name
  - ingestion status
  - service_instance_id should refer to **intake** (not engine)

### Routing semantics at ingress

DatasourceInstance can have **multiple outputs**.

- It behaves like a fan-out router at the start of the pipeline.
- Retry semantics apply to **dispatch/output** operations, not module execution.

### Engine owns routing rules

Engine continues to own:

- entry mapping from datasource instance → entry node
- node `filter_conditions`
- edge `condition` evaluation
- edge transport selection

Intake does not evaluate CEL.

## Ingress Modes

### Mode 1: HTTP Upload (always staged)

- HTTP POST is always **store-first**.
- Objective: never require the intake service to hold the entire binary in memory.

Flow:

1. Client uploads bytes to `connector-intake-service` via HTTP.
2. Intake streams upload to **repo-service** (repo-service performs object storage write).
3. Intake publishes a **reference-only** `PipeStream` to Kafka (via kafka-sidecar / Kafka producer path).
4. Engine processes from Kafka like any other message.

Key properties:

- Durable staging enables replay/recrawl.
- Payload size is bounded in memory.

### Mode 2: gRPC ingestion (inline allowed)

- For trusted clients, allow sending the payload inline over gRPC.
- Engine/modules still hydrate via repo-service when required by capabilities.

Optional:

- gRPC callers may also use staged refs (same as Mode 1) for large payloads.

## Storage References (Contract)

We want provider-neutral references between services.

- Repo-service owns object storage specifics.
- Other services exchange a repo-managed handle (e.g., `drive_name` + `object_key`).

Avoid embedding `s3://` semantics into widely-shared contracts unless we intentionally want other services to bypass repo-service (we do not).

## Proto / API Implications

Expected changes in `pipestream-protos`:

- Add a structured `IngestContext` message.
- Add a graph-level `DatasourceInstance` (or `DatasourceBinding`) model:
  - binds datasource instance id → entry node id
  - carries `custom_config` (JSON config + schema pointer)
  - carries ingestion policy fields
- Update intake-to-engine handoff to identify datasource instance (not just datasource id).

Note: we should keep backward compatibility where possible:

- `datasource_id` may remain as a stable identity; `datasource_instance_id` is graph-versioned.

## Downstream Service Impact

### connector-intake-service

- Remains thin:
  - auth/validation
  - streaming upload to repo-service for HTTP mode
  - constructs `PipeStream` with `IngestContext`
  - publishes to Kafka or calls Engine intakeHandoff (depending on ingress mode)

### repository-service

- Continues to own object storage interaction.
- May add/adjust APIs for streaming upload and returning repo-managed handles.

### pipestream-engine

- Must resolve datasource instance from active graph and stamp/validate `IngestContext`.
- Must treat datasource instance as a special ingress entity (not a processing node).
- Hop history should include ingress record (hop 0).

### engine-kafka-sidecar

- Likely no fundamental changes beyond ensuring ref-only messages flow correctly.

### modules/sinks

- May optionally read `IngestContext` hints (index hints, ACLs, retention, etc.).
- No direct object store access.

## Open Questions

- Exact proto location for `IngestContext`: `StreamMetadata` vs top-level `PipeStream`.
- How to version datasource instances within a graph (id scheme).
- Exact repository-service RPC to use for streaming upload (reuse existing NodeUploadService vs new dedicated method).

## Tickets (to create)

See the linked issues created from this doc for implementation sequencing across repos.
