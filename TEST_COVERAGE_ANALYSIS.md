# Engine Service Test Coverage Analysis

## Current Test Coverage

### ✅ Well Tested

1. **Blob Hydration Flow** (`EngineV1ServiceBlobHydrationTest`)
   - ✅ Parser module with blob hydration
   - ✅ Non-parser module without blob hydration
   - ✅ Blob hydration failure handling
   - ✅ Already hydrated blob handling
   - **Status**: Comprehensive coverage

2. **Module Capability Detection** (`ModuleCapabilityServiceTest`)
   - ✅ Parser capability detection
   - ✅ Non-parser capability detection
   - ✅ Capability caching
   - ✅ Error handling
   - **Status**: Comprehensive coverage

3. **Graph Cache** (`GraphCacheTest`)
   - ✅ Node storage and retrieval
   - ✅ Edge storage and sorting
   - ✅ Module storage
   - **Status**: Good unit test coverage

4. **Supporting Services**
   - ✅ Mapping service tests
   - ✅ Processing buffer tests
   - ✅ Descriptor registry tests
   - **Status**: Good coverage

---

## ❌ Missing Critical Tests

### 1. Multi-Node Pipeline Flow (HIGH PRIORITY)
**Why**: This is the core engine functionality - processing documents through multiple nodes.

**What's Missing**:
- Test complete pipeline: parser → chunker → embedder → sink
- Verify routing between nodes works correctly
- Verify metadata/history accumulates correctly
- Verify hop count increments properly

**Impact**: **CRITICAL** - This is the primary use case of the engine.

**Test File**: `EngineV1ServiceMultiNodePipelineTest.java`

**Estimated Effort**: Medium (2-3 hours)

---

### 2. CEL Routing Logic (HIGH PRIORITY)
**Why**: CEL conditions determine which edges a document follows. This is critical for conditional routing.

**What's Missing**:
- Test CEL evaluation with hydrated documents
- Test conditional routing (document follows edge A if condition true, edge B if false)
- Test multiple matching edges (fan-out)
- Test no matching edges (terminal node)
- Test CEL evaluation errors

**Impact**: **CRITICAL** - Routing is fundamental to pipeline execution.

**Test File**: `EngineV1ServiceCelRoutingTest.java`

**Estimated Effort**: Medium (2-3 hours)

---

### 3. Intake Handoff (MEDIUM PRIORITY)
**Why**: This is the entry point for documents into the engine from the Kafka sidecar.

**What's Missing**:
- Test intake handoff acceptance
- Test intake handoff rejection
- Test entry node routing
- Test datasource-based routing

**Impact**: **HIGH** - This is how documents enter the system.

**Test File**: `EngineV1ServiceIntakeHandoffTest.java`

**Estimated Effort**: Medium (2-3 hours)

---

### 4. SavePipeDoc After Processing (MEDIUM PRIORITY)
**Why**: Documents need to be persisted after processing for downstream consumption.

**What's Missing**:
- Test SavePipeDoc is called after node processing
- Test SavePipeDoc with cluster ID
- Test SavePipeDoc error handling
- Test SavePipeDoc for dehydration (removing inline blob data)

**Impact**: **MEDIUM** - Important for persistence and dehydration.

**Test File**: `EngineV1ServiceSavePipeDocTest.java` (or add to existing tests)

**Estimated Effort**: Low-Medium (1-2 hours)

---

### 5. Error Handling in Multi-Node Scenarios (MEDIUM PRIORITY)
**Why**: Errors can occur at any node, and the engine needs to handle them gracefully.

**What's Missing**:
- Test module failure in middle of pipeline
- Test GetBlob failure in middle of pipeline
- Test routing failure
- Test partial pipeline completion
- Test error propagation

**Impact**: **MEDIUM** - Important for production resilience.

**Test File**: `EngineV1ServiceErrorHandlingTest.java`

**Estimated Effort**: Medium (2-3 hours)

---

### 6. Cross-Cluster Routing (LOW PRIORITY)
**Why**: Documents may need to be routed to different clusters.

**What's Missing**:
- Test routeToCluster method
- Test cluster ID handling
- Test cross-cluster message routing

**Impact**: **LOW** - May not be used initially, but important for scale.

**Test File**: `EngineV1ServiceCrossClusterRoutingTest.java`

**Estimated Effort**: Medium (2-3 hours)

---

### 7. Streaming Processing (LOW PRIORITY)
**Why**: The engine supports streaming processing for high-throughput scenarios.

**What's Missing**:
- Test processStream bidirectional streaming
- Test streaming with multiple documents
- Test streaming error handling

**Impact**: **LOW** - Advanced feature, may not be used initially.

**Test File**: `EngineV1ServiceStreamingTest.java`

**Estimated Effort**: High (4-6 hours) - Streaming tests are complex

---

### 8. Health and Monitoring (LOW PRIORITY)
**Why**: Health checks are important for observability.

**What's Missing**:
- Test getHealth method
- Test health status reporting
- Test topic subscription management

**Impact**: **LOW** - Important for ops, but not critical for functionality.

**Test File**: `EngineV1ServiceHealthTest.java`

**Estimated Effort**: Low (1 hour)

---

## 🎯 Recommended Test Priority

### Must Have (Before Production)
1. **Multi-Node Pipeline Flow** - Core functionality
2. **CEL Routing Logic** - Critical for conditional routing

### Should Have (For Production Readiness)
3. **Intake Handoff** - Entry point for documents
4. **SavePipeDoc After Processing** - Persistence and dehydration
5. **Error Handling in Multi-Node Scenarios** - Resilience

### Nice to Have (For Complete Coverage)
6. **Cross-Cluster Routing** - Advanced feature
7. **Streaming Processing** - Advanced feature
8. **Health and Monitoring** - Observability

---

## 📊 Coverage Summary

**Current Coverage**:
- ✅ Blob hydration: **Excellent**
- ✅ Module capabilities: **Excellent**
- ✅ Single node processing: **Good** (via blob hydration tests)
- ❌ Multi-node pipeline: **Missing**
- ❌ CEL routing: **Missing**
- ❌ Intake handoff: **Missing**
- ❌ SavePipeDoc: **Missing**
- ❌ Error handling: **Partial**

**Overall Assessment**: **~40% coverage** of critical paths

**Recommendation**: Add multi-node pipeline and CEL routing tests before considering the engine "tested enough" for production.

---

## 🚀 Next Steps

1. **Create Multi-Node Pipeline Test** (2-3 hours)
   - Most critical missing test
   - Validates core engine functionality

2. **Create CEL Routing Test** (2-3 hours)
   - Critical for conditional routing
   - Validates routing logic

3. **Create Intake Handoff Test** (2-3 hours)
   - Entry point validation
   - Important for integration

**Total Estimated Effort**: 6-9 hours for critical tests

**After Critical Tests**: Engine will have ~70% coverage of critical paths, which is acceptable for production with monitoring and gradual rollout.


