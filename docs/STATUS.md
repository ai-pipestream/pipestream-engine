# Pipestream Engine & Repository Service - Current Status

**Last Updated**: 2025-01-XX

## ✅ Completed Features

### Repository Service (Phase 1 - Mostly Complete)

#### Core Persistence
- ✅ **SavePipeDoc**: Full implementation with UUID-based node_id, cluster_id support
- ✅ **GetPipeDoc**: By node_id (UUID primary key)
- ✅ **GetPipeDocByReference**: For DocumentReference hydration
- ✅ **GetBlob**: Level 2 hydration API (fetches raw blob bytes from S3)

#### Database & Storage
- ✅ **PostgreSQL Schema**: pipedocs table with UUID primary key, cluster_id, graph_address_id
- ✅ **S3 Path Structure**: UUID-based filenames, intake/cluster organization
- ✅ **Flyway Migrations**: All schema changes tracked

#### Upload Modes
- ✅ **HTTP POST Upload**: RawUploadResource for raw file uploads
- ✅ **UUID-based Blob Storage**: Blobs stored with UUID filenames, original filename in metadata

#### Events & Integration
- ✅ **Kafka Events**: RepositoryEventEmitter with lean events
- ✅ **UUID Key Extractor**: For deterministic Kafka partitioning

### Engine Service

#### Graph Management
- ✅ **PostgreSQL Storage**: PipelineGraph stored as JSONB
- ✅ **Graph CRUD**: Create, read, update, activate operations
- ✅ **Graph Cache**: In-memory cache with node/edge indexing
- ✅ **Graph gRPC Service**: Full CRUD API

#### Processing Loop
- ✅ **Level 1 Hydration**: DocumentReference → PipeDoc via RepoClient
- ✅ **Kafka Publishing**: Dehydration and publishing to node topics
- ✅ **gRPC Routing**: Direct gRPC calls to next nodes
- ✅ **CEL Evaluation**: Basic CEL support for routing conditions

#### Infrastructure
- ✅ **Dynamic gRPC Clients**: RepoClient using DynamicGrpcClientFactory
- ✅ **Kafka Integration**: Apicurio Protobuf serialization
- ✅ **Service Discovery**: Consul-based (via dynamic-grpc extension)

## 🚧 In Progress / Partially Complete

### Level 2 Blob Hydration (Engine)
- **Status**: API exists, not integrated into processing loop
- **What's Done**: 
  - `RepoClient.getBlob()` implemented
  - `RepositoryGrpcService.getBlob()` implemented
- **What's Missing**:
  - Module capability checking (does module need blob?)
  - Integration into `EngineV1Service.ensureHydration()`
  - Blob inlining logic (replace storage_ref with inline data)

### Graph Validation Service (Repository)
- **Status**: Placeholder implementation (always returns true)
- **What's Done**: Service structure exists
- **What's Missing**:
  - gRPC client to Engine's PipelineGraphService
  - Query active graphs for node ID existence
  - Proper error handling

## ❌ Not Started / Missing

### Module Capability Detection
- **Need**: Determine if a module requires blob content (e.g., parsers need it, chunkers don't)
- **Options**:
  1. Module proto definition (capability flags)
  2. Service discovery metadata (Consul tags)
  3. Module registry API
  4. Configuration in graph node definition

### CEL Hydration Fix
- **Issue**: CEL evaluator has TODO about hydration
- **Need**: Ensure documents are hydrated before CEL evaluation, but just the PipeDoc not the Blob.  Blob only hydrates if we are on a 
  PARSER module.
- **Location**: `CelEvaluatorService.evaluateValue()`

### Integration Tests
- **Need**: End-to-end tests covering:
  - Engine → Repo Service → S3 → Engine cycle
  - Kafka path: Repo → Kafka → Sidecar → Engine
  - Level 1 + Level 2 hydration flow
  - Graph validation integration

### WireMock Server Updates
- **Need**: Verify compatibility with latest proto changes
- **Need**: Add test scenarios for:
  - Module capability responses
  - Parser modules (require blob)
  - Non-parser modules (no blob needed)

### Additional Repository Service Features
- **ListPipeDocs**: TODO in RepositoryGrpcService
- **Metadata Service**: TODO placeholder
- **Version Control Service**: TODO placeholder
- **Cache Service**: TODO placeholder
- **Upload Workers**: TODO placeholders (for Phase 2 multipart upload)

## 📋 Next Steps (Priority Order)

### 1. Level 2 Blob Hydration (HIGH PRIORITY)
**Why**: Critical for parser modules to work. Without this, parsers can't access raw file bytes.

**Tasks**:
1. Determine module capability detection approach
2. Add capability check in `EngineV1Service.ensureHydration()`
3. Implement blob fetching and inlining logic
4. Test with a parser module (via WireMock)

**Estimated Effort**: 2-3 hours

### 2. Graph Validation Service (MEDIUM PRIORITY)
**Why**: Prevents invalid node IDs from being saved. Important for data integrity.

**Tasks**:
1. Add gRPC client to Engine's PipelineGraphService
2. Implement `GetActiveGraph()` call
3. Check if node_id exists in active graph
4. Add error handling and caching

**Estimated Effort**: 1-2 hours

### 3. Integration Tests (HIGH PRIORITY)
**Why**: Validate end-to-end flows work correctly. Critical before production use.

**Tasks**:
1. Create integration test suite
2. Test: Intake → Repo → Engine → Module → Repo cycle
3. Test: Kafka path with hydration
4. Test: Level 2 hydration with parser module

**Estimated Effort**: 4-6 hours

### 4. CEL Hydration Fix (MEDIUM PRIORITY)
**Why**: Ensures CEL expressions have access to document data.

**Tasks**:
1. Review CEL evaluator code
2. Ensure hydration happens before CEL evaluation
3. Add defensive checks

**Estimated Effort**: 1 hour

### 5. WireMock Server Updates (MEDIUM PRIORITY)
**Why**: Need realistic module mocks for testing.

**Tasks**:
1. Verify proto compatibility
2. Add module capability responses
3. Add parser module mock (requires blob)
4. Add non-parser module mock (no blob)

**Estimated Effort**: 2-3 hours

## 🎯 Phase 1 Completion Criteria

From `repository-service/docs/new-design/phase-1-savepipedoc.md`:

- ✅ SavePipeDoc persists and returns receipt
- ✅ Idempotent retry with same `(doc_id, checksum)` does not create duplicates
- ✅ Large blob triggers S3 offload + storage_ref replacement
- ✅ Event emitted on success (and not emitted on failed transaction)
- ⚠️ **Level 2 blob hydration integration** (API exists, needs integration)

## 📚 Design Document Status

### Repository Service
- ✅ `DESIGN.md` - Up to date
- ✅ `S3-PATH-STRUCTURE.md` - Up to date
- ✅ `docs/new-design/phase-1-savepipedoc.md` - Mostly complete
- ✅ `docs/new-design/phases.md` - Phase 1 mostly done

### Engine Service
- ✅ `docs/architecture/08-kafka-sidecar.md` - Complete (sidecar being built separately)
- ✅ `docs/architecture/05-hydration-model.md` - Level 1 done, Level 2 pending
- ✅ `docs/architecture/02-processing-loop.md` - Core loop done, Level 2 pending
- ✅ `docs/architecture/03-graph-management.md` - Complete

## 🔍 Testing Status

### Unit Tests
- ✅ Repository Service: Entity tests, service tests, gRPC tests
- ✅ Engine: Graph cache tests, graph service tests
- ⚠️ Engine: Processing loop tests (need module mocks)

### Integration Tests
- ❌ End-to-end Engine + Repo Service flow
- ❌ Kafka path with sidecar (pending sidecar completion)
- ❌ Level 2 hydration flow

## 🚀 Recommended Next Work Session

**Focus**: Level 2 Blob Hydration

1. **Decision**: Choose module capability detection approach
   - Option A: Add to graph node config (quickest)
   - Option B: Query module service (more dynamic)
   - Option C: Module proto definition (most robust)

2. **Implementation**: Integrate into `EngineV1Service.ensureHydration()`
   - Check if module needs blob
   - If yes and blob has storage_ref, fetch via `RepoClient.getBlob()`
   - Inline blob data, clear storage_ref

3. **Testing**: Use WireMock to test with parser module

**Estimated Time**: 2-3 hours for complete implementation + tests

## 📝 Notes

- **Kafka Sidecar**: Being built by another agent (requirements doc created)
- **Graph Validation**: Can be done in parallel with Level 2 hydration
- **Integration Tests**: Should be done after Level 2 hydration is complete
- **WireMock Updates**: Can be done incrementally as needed for testing



