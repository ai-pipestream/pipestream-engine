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

    /** Logger for this service class. */
    private static final Logger LOG = Logger.getLogger(CelEvaluatorService.class);

    /** The CEL (Common Expression Language) environment instance. */
    private Cel cel;

    /** Cache for compiled CEL scripts to improve performance. Maps condition string to compiled AST. */
    private final Map<String, CelAbstractSyntaxTree> scriptCache = new ConcurrentHashMap<>();

    /**
     * Initializes the CEL environment.
     * <p>
     * Sets up the CEL factory with protobuf type support for PipeDoc and PipeStream,
     * enabling evaluation of conditions against document and stream data.
     */
    @PostConstruct
    public void init() {
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
     * <p>
     * Compiles and caches CEL expressions for performance. The condition can reference
     * 'document' (the PipeDoc) and 'stream' (the PipeStream) variables. Returns false
     * on any evaluation error to ensure fail-safe routing behavior.
     *
     * @param condition The CEL expression string (e.g., "document.search_metadata.language == 'en'").
     *                  If null or empty, returns true (always match).
     * @param stream The current pipeline stream context containing document and metadata
     * @return true if the condition matches, false otherwise (or on evaluation error)
     */
    public boolean evaluate(String condition, PipeStream stream) {
        Object result = evaluateValue(condition, stream);
        if (result instanceof Boolean) {
            return (Boolean) result;
        } else if (result != null) {
            LOG.warnf("CEL condition '%s' did not return a boolean: %s", condition, result);
        }
        return false;
    }

    /**
     * Evaluates a CEL expression against a document and stream context and returns the raw result.
     * <p>
     * Compiles and caches CEL expressions for performance. The expression can reference
     * 'document' (the PipeDoc) and 'stream' (the PipeStream) variables.
     *
     * @param expression The CEL expression string (e.g., "document.search_metadata.relevance_score * 100").
     *                   If null or empty, returns null.
     * @param stream The current pipeline stream context containing document and metadata
     * @return The result of the evaluation, or null if expression is empty or on error
     */
    public Object evaluateValue(String expression, PipeStream stream) {
        if (expression == null || expression.isBlank()) {
            return null;
        }

        try {
            CelAbstractSyntaxTree ast = scriptCache.computeIfAbsent(expression, this::compile);
            CelRuntime.Program program = cel.createProgram(ast);

            Map<String, Object> input = new HashMap<>();
            // Extract document from stream (handles both inline and reference)
            // Note: CEL evaluation needs the full document, so if only a reference exists,
            // the caller should hydrate first. For now, we assume hydration happens before CEL evaluation.
            if (stream.hasDocument()) {
                input.put("document", stream.getDocument());
            } else if (stream.hasDocumentRef()) {
                // TODO: Should hydrate here, but for now log warning
                LOG.warnf("CEL evaluation called with document_ref - document should be hydrated first");
                input.put("document", null); // Will cause CEL evaluation to fail
            } else {
                LOG.warnf("CEL evaluation called with no document - stream may be in invalid state");
                input.put("document", null);
            }
            input.put("stream", stream);

            return program.eval(input);
        } catch (Exception e) {
            LOG.errorf("Error evaluating CEL expression '%s': %s", expression, e.getMessage());
            return null;
        }
    }

    /**
     * Compiles a CEL expression into an abstract syntax tree.
     * <p>
     * Uses the pre-configured CEL environment to parse and validate the expression.
     * Throws RuntimeException on validation errors to ensure invalid expressions
     * are caught early rather than at evaluation time.
     *
     * @param expression The CEL expression string to compile
     * @return The compiled abstract syntax tree ready for evaluation
     * @throws RuntimeException if the expression is invalid or cannot be compiled
     */
    private CelAbstractSyntaxTree compile(String expression) {
        try {
            return cel.compile(expression).getAst();
        } catch (CelValidationException e) {
            LOG.errorf("Invalid CEL expression: %s", expression, e);
            throw new RuntimeException("Invalid CEL expression: " + expression, e);
        }
    }
}
