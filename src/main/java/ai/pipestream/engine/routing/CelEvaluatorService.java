package ai.pipestream.engine.routing;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import dev.cel.bundle.Cel;
import dev.cel.bundle.CelFactory;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.SimpleType;
import dev.cel.runtime.CelRuntime;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;

/**
 * Service for evaluating CEL (Common Expression Language) conditions for routing.
 * <p>
 * This service compiles and caches CEL programs to determine which edges a document should follow.
 * The CEL environment is configured to accept 'document' (PipeDoc) and 'stream' (PipeStream) variables.
 */
@ApplicationScoped
public class CelEvaluatorService {

    private static final Logger LOG = Logger.getLogger(CelEvaluatorService.class);
    
    private Cel cel;
    
    // Cache for compiled scripts: "condition_string" -> CompiledAST
    private final Map<String, CelAbstractSyntaxTree> scriptCache = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        try {
            // Initialize CEL environment with standard library and protobuf type support
            cel = CelFactory.standardCelBuilder()
                    .addMessageTypes(PipeDoc.getDescriptor())
                    .addMessageTypes(PipeStream.getDescriptor())
                    .addVar("document", SimpleType.DYN) 
                    .addVar("stream", SimpleType.DYN)
                    .build();
            
            LOG.info("CEL Evaluator initialized successfully");
        } catch (Exception e) {
            LOG.error("Failed to initialize CEL environment", e);
            throw new RuntimeException("Failed to initialize CEL environment", e);
        }
    }

    /**
     * Evaluates a boolean condition against a document and stream context.
     * 
     * @param condition The CEL expression string (e.g., "document.search_metadata.language == 'en'")
     * @param stream The current pipeline stream context
     * @return true if the condition matches, false otherwise (or on error)
     */
    public boolean evaluate(String condition, PipeStream stream) {
        if (condition == null || condition.isBlank()) {
            return true; // Empty condition implies "always match"
        }

        try {
            CelAbstractSyntaxTree ast = scriptCache.computeIfAbsent(condition, this::compile);
            
            CelRuntime.Program program = cel.createProgram(ast);
            
            Map<String, Object> input = new HashMap<>();
            input.put("document", stream.getDocument());
            input.put("stream", stream);

            // Execute the script
            Object result = program.eval(input);
            
            if (result instanceof Boolean) {
                return (Boolean) result;
            } else {
                LOG.warnf("CEL condition '%s' did not return a boolean: %s", condition, result);
                return false;
            }

        } catch (Exception e) {
            LOG.errorf("Error evaluating CEL condition '%s': %s", condition, e.getMessage());
            return false; // Fail-safe: don't route if we can't evaluate
        }
    }

    private CelAbstractSyntaxTree compile(String expression) {
        try {
            return cel.compile(expression).getAst();
        } catch (CelValidationException e) {
            LOG.errorf("Invalid CEL expression: %s", expression, e);
            throw new RuntimeException("Invalid CEL expression: " + expression, e);
        }
    }
}
