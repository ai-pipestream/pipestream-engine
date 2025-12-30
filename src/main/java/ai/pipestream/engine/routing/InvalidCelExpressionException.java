package ai.pipestream.engine.routing;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Exception thrown when a CEL (Common Expression Language) expression is invalid.
 * <p>
 * This exception is thrown during CEL compilation when the expression has syntax errors
 * or references undefined variables/functions. It maps to gRPC INVALID_ARGUMENT status
 * for proper error propagation to clients.
 * <p>
 * The exception preserves the original expression and the underlying compilation error
 * to aid in debugging configuration issues.
 */
public class InvalidCelExpressionException extends StatusRuntimeException {

    private final String expression;
    private final String compilationError;

    /**
     * Creates an InvalidCelExpressionException with the invalid expression and cause.
     *
     * @param expression The CEL expression that failed to compile
     * @param cause The underlying exception from the CEL compiler
     */
    public InvalidCelExpressionException(String expression, Throwable cause) {
        super(Status.INVALID_ARGUMENT
                .withDescription(buildMessage(expression, cause))
                .withCause(cause));
        this.expression = expression;
        this.compilationError = cause != null ? cause.getMessage() : "Unknown error";
    }

    /**
     * Creates an InvalidCelExpressionException with the invalid expression and error message.
     *
     * @param expression The CEL expression that failed to compile
     * @param errorMessage The error message describing why compilation failed
     */
    public InvalidCelExpressionException(String expression, String errorMessage) {
        super(Status.INVALID_ARGUMENT
                .withDescription(buildMessage(expression, errorMessage)));
        this.expression = expression;
        this.compilationError = errorMessage;
    }

    /**
     * Builds a helpful error message with the expression and compilation error.
     */
    private static String buildMessage(String expression, Throwable cause) {
        String errorDetail = cause != null ? cause.getMessage() : "Unknown compilation error";
        return buildMessage(expression, errorDetail);
    }

    /**
     * Builds a helpful error message with the expression and error detail.
     */
    private static String buildMessage(String expression, String errorDetail) {
        // Truncate very long expressions for readability
        String displayExpression = expression;
        if (expression != null && expression.length() > 100) {
            displayExpression = expression.substring(0, 97) + "...";
        }
        return String.format("Invalid CEL expression '%s': %s", displayExpression, errorDetail);
    }

    /**
     * Returns the CEL expression that failed to compile.
     *
     * @return The invalid expression string
     */
    public String getExpression() {
        return expression;
    }

    /**
     * Returns the compilation error message.
     *
     * @return The error message from the CEL compiler
     */
    public String getCompilationError() {
        return compilationError;
    }
}
