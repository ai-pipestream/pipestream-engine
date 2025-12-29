package ai.pipestream.engine.module;

import ai.pipestream.data.module.v1.*;
import ai.pipestream.engine.graph.GraphCache;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for querying and caching module capabilities.
 * <p>
 * This service determines whether a module requires blob content (e.g., parsers)
 * or can work with parsed metadata only (e.g., chunkers, embedders).
 * <p>
 * Capabilities are queried from modules via their {@code GetServiceRegistration()} RPC
 * and cached to avoid repeated queries. The cache is keyed by module ID.
 * <p>
 * The primary use case is to determine whether Level 2 blob hydration is needed
 * before calling a module.
 */
@ApplicationScoped
public class ModuleCapabilityService {

    /** Logger for this service class. */
    private static final Logger LOG = Logger.getLogger(ModuleCapabilityService.class);

    /** Injected factory for creating dynamic gRPC clients for module communication. */
    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    /** Injected cache for pipeline graph topology to resolve module service names. */
    @Inject
    GraphCache graphCache;

    /**
     * Cache of module capabilities keyed by module ID.
     * <p>
     * This cache stores the result of {@code GetServiceRegistration()} calls
     * to avoid repeated queries for the same module. The cache is thread-safe
     * and will be populated on-demand when capabilities are first requested.
     */
    private final Map<String, Optional<Capabilities>> capabilityCache = new ConcurrentHashMap<>();

    /**
     * Determines whether a module requires blob content (Level 2 hydration).
     * <p>
     * This method checks if the module has the {@code CAPABILITY_TYPE_PARSER} capability,
     * which indicates it needs raw binary content to parse documents.
     * <p>
     * The decision logic:
     * <ul>
     *   <li>If module has {@code CAPABILITY_TYPE_PARSER} → returns {@code true} (needs blob)</li>
     *   <li>If module has no capabilities or other capabilities → returns {@code false} (no blob needed)</li>
     *   <li>If module is not found or query fails → returns {@code false} (safe default, no hydration)</li>
     * </ul>
     *
     * @param moduleId The module ID to check
     * @return A Uni that completes with {@code true} if the module requires blob content, {@code false} otherwise
     */
    public Uni<Boolean> requiresBlobContent(String moduleId) {
        return getCapabilities(moduleId)
                .map(capsOpt -> {
                    if (capsOpt.isEmpty()) {
                        LOG.debugf("Module %s has no capabilities - defaulting to no blob hydration", moduleId);
                        return false;
                    }
                    
                    Capabilities caps = capsOpt.get();
                    boolean hasParser = caps.getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER);
                    
                    if (hasParser) {
                        LOG.debugf("Module %s has PARSER capability - blob hydration required", moduleId);
                    } else {
                        LOG.debugf("Module %s does not have PARSER capability - no blob hydration needed", moduleId);
                    }
                    
                    return hasParser;
                })
                .onFailure().recoverWithItem(throwable -> {
                    LOG.warnf(throwable, "Failed to determine capabilities for module %s - defaulting to no blob hydration", moduleId);
                    return false; // Safe default: don't hydrate if we can't determine capabilities
                });
    }

    /**
     * Gets the capabilities for a module, using cache if available.
     * <p>
     * This method first checks the in-memory cache. If not found, it queries
     * the module via {@code GetServiceRegistration()} and caches the result.
     * <p>
     * The service name is resolved from the graph cache using the module ID,
     * falling back to the module ID itself if not found in the graph.
     *
     * @param moduleId The module ID to query
     * @return A Uni that completes with an Optional containing the module's capabilities,
     *         or empty if the module is not found or does not report capabilities
     */
    public Uni<Optional<Capabilities>> getCapabilities(String moduleId) {
        if (moduleId == null) {
            return Uni.createFrom().item(Optional.empty());
        }

        // Check cache first
        Optional<Capabilities> cached = capabilityCache.get(moduleId);
        if (cached != null) {
            LOG.debugf("Using cached capabilities for module %s", moduleId);
            return Uni.createFrom().item(cached);
        }

        // Resolve service name from graph cache
        String serviceName = graphCache.getModule(moduleId)
                .map(module -> {
                    String grpcServiceName = module.getGrpcServiceName();
                    return (grpcServiceName != null && !grpcServiceName.isEmpty()) 
                            ? grpcServiceName 
                            : moduleId;
                })
                .orElse(moduleId);

        LOG.debugf("Querying capabilities for module %s (service: %s)", moduleId, serviceName);

        // Query module via GetServiceRegistration with x-module-name header
        // This allows WireMock to return module-specific capabilities
        GetServiceRegistrationRequest request = GetServiceRegistrationRequest.newBuilder().build();

        // Create metadata with x-module-name header for WireMock module-specific routing
        Metadata metadata = new Metadata();
        Metadata.Key<String> moduleNameKey = Metadata.Key.of("x-module-name", Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(moduleNameKey, moduleId);

        // Get channel and create stub with metadata interceptor
        return grpcClientFactory.getChannel(serviceName)
                .map(channel -> {
                    // Add metadata interceptor to attach x-module-name header
                    Channel interceptedChannel = ClientInterceptors.intercept(
                            channel,
                            MetadataUtils.newAttachHeadersInterceptor(metadata)
                    );
                    return MutinyPipeStepProcessorServiceGrpc.newMutinyStub(interceptedChannel);
                })
                .flatMap(stub -> stub.getServiceRegistration(request))
                .map(response -> {
                    if (response.hasCapabilities()) {
                        Capabilities caps = response.getCapabilities();
                        // Cache the result
                        capabilityCache.put(moduleId, Optional.of(caps));
                        LOG.debugf("Cached capabilities for module %s: %s", moduleId, caps.getTypesList());
                        return Optional.of(caps);
                    } else {
                        // Module doesn't report capabilities - cache empty result
                        capabilityCache.put(moduleId, Optional.empty());
                        LOG.debugf("Module %s does not report capabilities", moduleId);
                        return Optional.<Capabilities>empty();
                    }
                })
                .onFailure().recoverWithUni(throwable -> {
                    LOG.warnf(throwable, "Failed to query capabilities for module %s (service: %s)", moduleId, serviceName);
                    // Cache empty result to avoid repeated failed queries
                    capabilityCache.put(moduleId, Optional.empty());
                    return Uni.createFrom().item(Optional.<Capabilities>empty());
                });
    }

    /**
     * Clears the capability cache for a specific module.
     * <p>
     * This is useful when a module is updated or re-registered and its
     * capabilities may have changed.
     *
     * @param moduleId The module ID to clear from cache
     */
    public void clearCache(String moduleId) {
        capabilityCache.remove(moduleId);
        LOG.debugf("Cleared capability cache for module %s", moduleId);
    }

    /**
     * Clears the entire capability cache.
     * <p>
     * This is useful for testing or when modules are updated in bulk.
     */
    public void clearAllCache() {
        capabilityCache.clear();
        LOG.debugf("Cleared all capability cache entries");
    }
}

