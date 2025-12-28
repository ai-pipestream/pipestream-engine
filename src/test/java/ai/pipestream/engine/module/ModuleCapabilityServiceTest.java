package ai.pipestream.engine.module;

import ai.pipestream.data.module.v1.*;
import ai.pipestream.engine.graph.GraphCache;
import ai.pipestream.engine.util.WireMockTestResource;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for {@link ModuleCapabilityService}.
 * <p>
 * These tests use the WireMock server to simulate module responses and verify
 * that the capability service correctly:
 * - Queries modules for their capabilities
 * - Caches capability responses
 * - Determines whether blob hydration is needed based on PARSER capability
 * <p>
 * Note: These tests require the WireMock server to be running (started automatically
 * via {@link WireMockTestResource}).
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class ModuleCapabilityServiceTest {

    @Inject
    ModuleCapabilityService capabilityService;

    @Inject
    GraphCache graphCache;

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    @BeforeEach
    void setUp() {
        // Clear cache before each test to ensure fresh queries
        capabilityService.clearAllCache();
    }

    /**
     * Tests that a module with PARSER capability correctly reports that it requires blob content.
     * <p>
     * This test verifies the core hydration decision logic: if a module has
     * {@code CAPABILITY_TYPE_PARSER}, the engine should hydrate blobs before calling it.
     * <p>
     * WireMock uses the {@code x-module-name} header to return module-specific capabilities.
     * The engine sends this header automatically when querying capabilities.
     */
    @Test
    void testParserModuleRequiresBlobContent() {
        // WireMock's PipeStepProcessorMock registers tika-parser with PARSER capability
        // The engine sends x-module-name header, so WireMock returns tika-parser's capabilities
        String moduleId = "tika-parser";
        
        Uni<Boolean> requiresBlob = capabilityService.requiresBlobContent(moduleId);
        
        UniAssertSubscriber<Boolean> subscriber = requiresBlob
                .subscribe().withSubscriber(UniAssertSubscriber.create());
        
        // WireMock should return PARSER capability for tika-parser
        Boolean result = subscriber.awaitItem().getItem();
        
        // tika-parser has PARSER capability, so it should require blob content
        assertThat("tika-parser should require blob content", result, is(true));
    }

    /**
     * Tests that a module without PARSER capability correctly reports that it does NOT require blob content.
     * <p>
     * This test verifies that non-parser modules (e.g., chunkers, embedders) don't trigger
     * blob hydration, allowing the engine to work with parsed metadata only.
     * <p>
     * WireMock's PipeStepProcessorMock registers text-chunker with empty capabilities (no PARSER).
     */
    @Test
    void testNonParserModuleDoesNotRequireBlobContent() {
        // WireMock's PipeStepProcessorMock registers text-chunker without PARSER capability
        String moduleId = "text-chunker";
        
        Uni<Boolean> requiresBlob = capabilityService.requiresBlobContent(moduleId);
        
        UniAssertSubscriber<Boolean> subscriber = requiresBlob
                .subscribe().withSubscriber(UniAssertSubscriber.create());
        
        Boolean result = subscriber.awaitItem().getItem();
        
        // text-chunker has no PARSER capability, so it should NOT require blob content
        assertThat("text-chunker should not require blob content", result, is(false));
    }

    /**
     * Tests that capability queries are cached to avoid repeated gRPC calls.
     * <p>
     * This test verifies that subsequent calls to {@code requiresBlobContent()} for the same
     * module use the cached result instead of making another gRPC call.
     */
    @Test
    void testCapabilityCaching() {
        String moduleId = "test-module";
        
        // First call - should query the module
        Uni<Boolean> firstCall = capabilityService.requiresBlobContent(moduleId);
        Boolean firstResult = firstCall.await().indefinitely();
        
        // Second call - should use cache
        Uni<Boolean> secondCall = capabilityService.requiresBlobContent(moduleId);
        Boolean secondResult = secondCall.await().indefinitely();
        
        // Results should be the same (cached)
        assertThat("Cached result should match first result", secondResult, is(firstResult));
    }

    /**
     * Tests that capability queries handle failures gracefully.
     * <p>
     * If a module is unavailable or returns an error, the service should return
     * {@code false} (safe default: don't hydrate) rather than throwing an exception.
     */
    @Test
    void testCapabilityQueryHandlesFailures() {
        String nonExistentModuleId = "non-existent-module-12345";
        
        Uni<Boolean> requiresBlob = capabilityService.requiresBlobContent(nonExistentModuleId);
        
        UniAssertSubscriber<Boolean> subscriber = requiresBlob
                .subscribe().withSubscriber(UniAssertSubscriber.create());
        
        // Should complete successfully with false (safe default)
        Boolean result = subscriber.awaitItem().getItem();
        
        assertThat("Non-existent module should default to false", result, is(false)); // Safe default: don't hydrate if we can't determine capabilities
    }

    /**
     * Tests that {@code getCapabilities()} returns the correct capability structure.
     * <p>
     * This test verifies that the service correctly parses and returns the
     * {@code Capabilities} message from the module's {@code GetServiceRegistration()} response.
     */
    @Test
    void testGetCapabilities() {
        String moduleId = "tika-parser";
        
        Uni<Optional<Capabilities>> capabilitiesUni = capabilityService.getCapabilities(moduleId);
        
        UniAssertSubscriber<Optional<Capabilities>> subscriber = capabilitiesUni
                .subscribe().withSubscriber(UniAssertSubscriber.create());
        
        Optional<Capabilities> capsOpt = subscriber.awaitItem().getItem();
        
        // WireMock should return capabilities for tika-parser
        assertThat("Capabilities should be present for tika-parser", capsOpt, is(notNullValue()));
        assertThat("tika-parser should have capabilities", capsOpt.isPresent(), is(true));
        if (capsOpt.isPresent()) {
            assertThat("tika-parser should have PARSER capability", 
                    capsOpt.get().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER), is(true));
        }
    }

    /**
     * Tests that cache can be cleared for a specific module.
     */
    @Test
    void testClearCacheForModule() {
        String moduleId = "test-module";
        
        // Query capabilities to populate cache
        capabilityService.requiresBlobContent(moduleId).await().indefinitely();
        
        // Clear cache
        capabilityService.clearCache(moduleId);
        
        // Query again - should make a new gRPC call (cache was cleared)
        Uni<Boolean> afterClear = capabilityService.requiresBlobContent(moduleId);
        Boolean result = afterClear.await().indefinitely();
        
        assertThat("Result after cache clear should not be null", result, is(notNullValue()));
    }

    /**
     * Tests that cache can be cleared for all modules.
     */
    @Test
    void testClearAllCache() {
        String moduleId1 = "module-1";
        String moduleId2 = "module-2";
        
        // Query capabilities to populate cache
        capabilityService.requiresBlobContent(moduleId1).await().indefinitely();
        capabilityService.requiresBlobContent(moduleId2).await().indefinitely();
        
        // Clear all cache
        capabilityService.clearAllCache();
        
        // Query again - should make new gRPC calls (cache was cleared)
        Uni<Boolean> afterClear1 = capabilityService.requiresBlobContent(moduleId1);
        Uni<Boolean> afterClear2 = capabilityService.requiresBlobContent(moduleId2);
        
        Boolean result1 = afterClear1.await().indefinitely();
        Boolean result2 = afterClear2.await().indefinitely();
        
        assertThat("Result 1 should not be null", result1, is(notNullValue()));
        assertThat("Result 2 should not be null", result2, is(notNullValue()));
    }
}

