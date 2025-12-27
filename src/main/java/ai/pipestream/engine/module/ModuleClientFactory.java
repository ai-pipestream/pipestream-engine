package ai.pipestream.engine.module;

import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;

/**
 * Factory for creating and caching gRPC clients for dynamic modules.
 * <p>
 * Uses Stork for service discovery (e.g. "stork://my-service").
 */
@ApplicationScoped
public class ModuleClientFactory {

    /** Logger for this service class. */
    private static final Logger LOG = Logger.getLogger(ModuleClientFactory.class);

    /** Cache of gRPC channels keyed by service name for connection reuse. */
    private final Map<String, ManagedChannel> channelCache = new ConcurrentHashMap<>();

    /**
     * Gets or creates a blocking stub for the specified module service.
     * <p>
     * Uses Stork service discovery to resolve the service name to an endpoint.
     * Channels are cached for performance and reused across calls.
     *
     * @param serviceName The name of the service (as registered in Consul)
     * @return The blocking stub for the module, ready for synchronous calls
     */
    public PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub getBlockingStub(String serviceName) {
        ManagedChannel channel = getChannel(serviceName);
        return PipeStepProcessorServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Gets or creates an async stub for the specified module service.
     * <p>
     * Uses Stork service discovery to resolve the service name to an endpoint.
     * Channels are cached for performance and reused across calls.
     *
     * @param serviceName The name of the service (as registered in Consul)
     * @return The async (Mutiny) stub for the module, ready for reactive calls
     */
    public PipeStepProcessorServiceGrpc.PipeStepProcessorServiceStub getAsyncStub(String serviceName) {
        ManagedChannel channel = getChannel(serviceName);
        return PipeStepProcessorServiceGrpc.newStub(channel);
    }

    /**
     * Gets or creates a gRPC channel for the specified service.
     * <p>
     * Uses a "stork://" target URI to enable service discovery through Stork.
     * Channels are cached to avoid connection overhead on repeated calls.
     *
     * @param serviceName The service name to connect to (will be resolved via Stork)
     * @return A managed channel for making gRPC calls to the service
     */
    private ManagedChannel getChannel(String serviceName) {
        return channelCache.computeIfAbsent(serviceName, name -> {
            LOG.infof("Creating new gRPC channel for module service: %s", name);
            // Use Stork for service discovery
            return ManagedChannelBuilder.forTarget("stork://" + name)
                    .usePlaintext()
                    .build();
        });
    }

    /**
     * Cleans up cached gRPC channels when the application shuts down.
     * <p>
     * Ensures proper resource cleanup by shutting down all cached channels.
     * Called automatically by CDI before the bean is destroyed.
     */
    @PreDestroy
    public void cleanup() {
        LOG.info("Shutting down module channels...");
        channelCache.values().forEach(channel -> {
            try {
                if (!channel.isShutdown()) {
                    channel.shutdownNow();
                }
            } catch (Exception e) {
                LOG.warn("Error shutting down channel", e);
            }
        });
        channelCache.clear();
    }
}
