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
        wireMockContainer = new GenericContainer<>(DockerImageName.parse("docker.io/pipestreamai/pipestream-wiremock-server:0.1.26"))
                .withExposedPorts(8080, 50052)
                .waitingFor(Wait.forLogMessage(".*WireMock Server started.*", 1));
        
        wireMockContainer.start();

        String host = wireMockContainer.getHost();
        String standardPort = wireMockContainer.getMappedPort(8080).toString();
        String directPort = wireMockContainer.getMappedPort(50052).toString();
        String moduleAddress = host + ":" + standardPort;

        return Map.of(
            // Stork static discovery for dynamic-grpc module services
            // Service names must match what ModuleCapabilityService uses (moduleId or grpcServiceName from graph)
            // For tests, we use the module ID directly as the service name
            "stork.tika-parser.service-discovery.type", "static",
            "stork.tika-parser.service-discovery.address-list", moduleAddress,
            
            "stork.text-chunker.service-discovery.type", "static",
            "stork.text-chunker.service-discovery.address-list", moduleAddress,
            
            "stork.opensearch-sink.service-discovery.type", "static",
            "stork.opensearch-sink.service-discovery.address-list", moduleAddress,
            
            // Repository service discovery (for hydration tests)
            "stork.repository-service.service-discovery.type", "static",
            "stork.repository-service.service-discovery.address-list", moduleAddress,
            
            // Registration service config - use the direct server port
            "pipestream.registration.registration-service.host", host,
            "pipestream.registration.registration-service.port", directPort
        );
    }

    @Override
    public void stop() {
        if (wireMockContainer != null) {
            wireMockContainer.stop();
        }
    }
}

