package ai.pipestream.engine.mapping.util.interceptor;

import com.google.protobuf.Message;
import jakarta.enterprise.util.Nonbinding;
import jakarta.interceptor.InterceptorBinding;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to enable automatic processing buffer capture for pipeline methods.
 * <p>
 * When applied to a method that returns ProcessResponse, this annotation will:
 * 1. Automatically capture the output document (PipeDoc) from ProcessResponse
 * 2. Store it in a ProcessingBuffer when enabled via configuration
 * 3. Provide seamless test data capture without manual buffer management
 * <p>
 * Configuration is controlled via properties:
 * - processing.buffer.enabled: Enable/disable buffer capture (default: false)
 * - processing.buffer.capacity: Buffer capacity (default: 100)
 * - processing.buffer.directory: Save directory (default: target/test-data)
 * <p>
 * Example usage:
 * <pre>
 * {@code
 * @ProcessingBuffered(type = PipeDoc.class, enabled = "${processing.buffer.enabled:false}")
 * public Uni<ProcessResponse> processData(ProcessRequest request) {
 *     // Normal processing logic - buffer capture is automatic
 *     return Uni.createFrom().item(response);
 * }
 * }
 * </pre>
 */
@InterceptorBinding
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface ProcessingBuffered {
    
    /**
     * The type of protobuf message to capture from the ProcessResponse.
     * Currently supports PipeDoc.class for output document capture.
     */
    @Nonbinding
    Class<? extends Message> type();
    
    /**
     * Whether buffer capture is enabled.
     * Supports configuration expressions like "${processing.buffer.enabled:false}".
     * Default is false to avoid unexpected buffer usage.
     */
    @Nonbinding
    String enabled() default "false";
    
    /**
     * Buffer capacity override.
     * Supports configuration expressions like "${processing.buffer.capacity:100}".
     * If not specified, uses the global configuration or default value.
     */
    @Nonbinding
    String capacity() default "";
    
    /**
     * Buffer save directory override.
     * Supports configuration expressions like "${processing.buffer.directory:target/test-data}".
     * If not specified, uses the global configuration or default value.
     */
    @Nonbinding
    String directory() default "";
    
    /**
     * Buffer file prefix for saved files.
     * Supports configuration expressions like "${processing.buffer.prefix:processed}".
     * If not specified, uses the module name as prefix.
     */
    @Nonbinding
    String prefix() default "";
}