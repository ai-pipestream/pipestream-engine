# Next Steps for Engine Development

## ✅ Completed

1. **Level 2 Blob Hydration** - ✅ Implemented
   - Module capability detection (GetServiceRegistration with header-based routing)
   - Blob hydration based on PARSER capability
   - GetBlob integration for fetching blob bytes
   - All tests passing with WireMock 0.1.26

2. **Module Capability Service** - ✅ Implemented
   - Header-based module querying (`x-module-name`)
   - Capability caching
   - Integration tests passing

3. **WireMock Integration** - ✅ Complete
   - WireMock 0.1.26 with GetBlob and SavePipeDoc support
   - All engine tests passing

## 🎯 Recommended Next Steps (Priority Order)

### Priority 1: End-to-End Integration Test for Blob Hydration Flow
**Why**: Verify the complete flow works end-to-end before moving to other features.

**What to test**:
1. Engine receives document with blob storage reference (no inline data)
2. Engine queries module capabilities (GetServiceRegistration with header)
3. Engine determines blob hydration is needed (PARSER capability)
4. Engine calls GetBlob to fetch blob bytes
5. Engine calls ProcessData with hydrated blob
6. Module processes successfully

**Test file to create**:
- `src/test/java/ai/pipestream/engine/EngineV1ServiceBlobHydrationTest.java`

**Test scenarios**:
- Parser module workflow with blob hydration (full flow)
- Non-parser module workflow without blob hydration (skips GetBlob)
- Blob hydration failure handling (GetBlob returns NOT_FOUND)
- Blob hydration retry logic (GetBlob returns UNAVAILABLE, then succeeds)

**Estimated effort**: Medium (2-3 hours)

---

### Priority 2: Fix CEL Hydration TODO
**Why**: Defensive programming - ensure CEL evaluation always has hydrated documents.

**Current issue**: `CelEvaluatorService.evaluateValue()` has a TODO on line 109 that warns when called with `document_ref` instead of hydrated document.

**What to fix**:
- In `routeToNextNodes()`, ensure document is hydrated before CEL evaluation
- Or add defensive hydration in `CelEvaluatorService` if document_ref is present
- Remove the TODO and warning

**Code location**:
- `src/main/java/ai/pipestream/engine/routing/CelEvaluatorService.java` (line 109)
- `src/main/java/ai/pipestream/engine/EngineV1Service.java` (line 365 - where CEL is called)

**Note**: Currently, `routeToNextNodes()` is called after `ensureHydration()`, so documents should already be hydrated. However, the defensive check would make the code more robust.

**Estimated effort**: Low (30 minutes - 1 hour)

---

### Priority 3: GraphValidationService Implementation
**Why**: Validate node IDs before processing to catch configuration errors early.

**What to implement**:
- Service that calls Engine's `PipelineGraphService` to validate node IDs
- Check if node exists in graph before processing
- Return clear error messages for invalid nodes

**Code location**: New service file
- `src/main/java/ai/pipestream/engine/validation/GraphValidationService.java`

**Estimated effort**: Medium (2-3 hours)

---

### Priority 4: Additional Integration Tests
**Why**: Comprehensive test coverage for production readiness.

**Test scenarios to add**:
- Multi-node pipeline flow (parser → chunker → embedder → sink)
- Error handling in multi-node pipeline
- CEL routing with hydrated documents
- SavePipeDoc after processing
- Large blob handling in end-to-end flow

**Estimated effort**: High (4-6 hours)

---

## 🎯 Recommended Immediate Next Step

**Create End-to-End Integration Test for Blob Hydration Flow**

This will:
1. Verify the complete blob hydration flow works correctly
2. Catch any integration issues early
3. Provide confidence that the feature is production-ready
4. Serve as documentation of how the feature works

**Test structure**:
```java
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class EngineV1ServiceBlobHydrationTest {
    
    @Test
    void testParserModuleWithBlobHydration() {
        // 1. Create PipeDoc with blob storage reference (no inline data)
        // 2. Register blob in WireMock
        // 3. Register parser module in WireMock
        // 4. Call engine processNodeLogic
        // 5. Verify GetBlob was called
        // 6. Verify ProcessData was called with hydrated blob
    }
    
    @Test
    void testNonParserModuleWithoutBlobHydration() {
        // Verify GetBlob is NOT called for non-parser modules
    }
}
```

---

## 📋 Summary

**Immediate Next Step**: Create end-to-end integration test for blob hydration flow

**Why**: Validates the complete feature works end-to-end and catches integration issues early.

**After that**: Fix CEL hydration TODO (quick win), then GraphValidationService (if needed).


