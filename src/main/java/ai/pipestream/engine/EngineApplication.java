package ai.pipestream.engine;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.jboss.logging.Logger;

/**
 * Main entry point for the Pipestream Engine service.
 * 
 * The Engine is the central orchestration service that routes documents through processing pipelines.
 * It receives documents via gRPC (IntakeHandoff or ProcessNode) and coordinates their flow through
 * the pipeline graph, invoking remote modules as needed.
 */
@QuarkusMain
public class EngineApplication implements QuarkusApplication {

    private static final Logger LOG = Logger.getLogger(EngineApplication.class);

    @Override
    public int run(String... args) {
        LOG.info("Starting Pipestream Engine service...");
        Quarkus.waitForExit();
        return 0;
    }

    public static void main(String... args) {
        Quarkus.run(EngineApplication.class, args);
    }
}

