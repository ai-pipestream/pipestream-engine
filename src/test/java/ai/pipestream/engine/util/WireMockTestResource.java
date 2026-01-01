package ai.pipestream.engine.util;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

/**
 * Test resource that starts a WireMock server container for integration testing.
 * <p>
 * This resource automatically launches the pipestream-wiremock-server Docker container
 * and configures Quarkus to use it for module service discovery and gRPC calls.
 * <p>
 * The WireMock server exposes:
 * - Port 8080: Standard gRPC server (reflection-enabled) for most services
 * - Port 50052: Direct streaming gRPC server for large streaming scenarios
 * <p>
 * This follows the same pattern used in other Pipestream services (repository-service,
 * account-service, etc.).
 */
public class WireMockTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> wireMockContainer;

    @SuppressWarnings("resource")
    @Override
    public Map<String, String> start() {
        // NOTE: pipestream-wiremock-server exposes multiple endpoints:
        // - Port 8080: gRPC server exposing many services incl. AccountService, modules (reflection-enabled)
        // - Port 50052: "Direct" streaming gRPC server (used for large streaming, and registration in some tests)
        wireMockContainer = new GenericContainer<>(DockerImageName.parse("docker.io/pipestreamai/pipestream-wiremock-server:0.1.28"))
                .withExposedPorts(8080, 50052)
                .waitingFor(Wait.forLogMessage(".*WireMock Server started.*", 1));
        
        wireMockContainer.start();

        String host = wireMockContainer.getHost();
        String standardPort = wireMockContainer.getMappedPort(8080).toString();
        String directPort = wireMockContainer.getMappedPort(50052).toString();
        String moduleAddress = host + ":" + standardPort;

        // Build configuration map - need more than 10 entries so use HashMap
        java.util.Map<String, String> config = new java.util.HashMap<>();

        // Stork static discovery for dynamic-grpc module services
        // Service names must match what ModuleCapabilityService uses (moduleId or grpcServiceName from graph)
        // For tests, we use the module ID directly as the service name
        config.put("stork.tika-parser.service-discovery.type", "static");
        config.put("stork.tika-parser.service-discovery.address-list", moduleAddress);

        config.put("stork.text-chunker.service-discovery.type", "static");
        config.put("stork.text-chunker.service-discovery.address-list", moduleAddress);

        config.put("stork.opensearch-sink.service-discovery.type", "static");
        config.put("stork.opensearch-sink.service-discovery.address-list", moduleAddress);

        // Generic test module names used by ModuleCapabilityServiceTest
        config.put("stork.test-module.service-discovery.type", "static");
        config.put("stork.test-module.service-discovery.address-list", moduleAddress);

        config.put("stork.module-1.service-discovery.type", "static");
        config.put("stork.module-1.service-discovery.address-list", moduleAddress);

        config.put("stork.module-2.service-discovery.type", "static");
        config.put("stork.module-2.service-discovery.address-list", moduleAddress);

        // Repository service discovery (for hydration tests)
        config.put("stork.repository-service.service-discovery.type", "static");
        config.put("stork.repository-service.service-discovery.address-list", moduleAddress);

        // Registration service config - use the direct server port
        config.put("pipestream.registration.registration-service.host", host);
        config.put("pipestream.registration.registration-service.port", directPort);

        return config;
    }

    @Override
    public void stop() {
        if (wireMockContainer != null) {
            wireMockContainer.stop();
        }
    }
}

