package ai.pipestream.engine.mapping.util.interceptor;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.engine.mapping.util.ProcessingBuffer;
import ai.pipestream.engine.mapping.util.ProcessingBufferFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Interceptor that automatically captures processing output to buffers based on @ProcessingBuffered annotation.
 * <p>
 * This interceptor:
 * 1. Intercepts methods annotated with @ProcessingBuffered
 * 2. Creates and manages ProcessingBuffer instances based on configuration
 * 3. Automatically captures output documents from ProcessResponse when enabled
 * 4. Provides seamless integration without manual buffer management
 * <p>
 * Configuration is resolved from annotation parameters, supporting config expressions.
 * Buffers are created lazily and cached per method to avoid recreation overhead.
 */
@ProcessingBuffered(type = PipeDoc.class) // Required for interceptor binding
@Interceptor
@Priority(Interceptor.Priority.APPLICATION + 100) // Run after other business logic
public class ProcessingBufferInterceptor {

    private static final Logger LOG = Logger.getLogger(ProcessingBufferInterceptor.class);
    
    // Cache buffers per method to avoid recreating them - static for access from other beans
    private static final ConcurrentMap<String, ProcessingBuffer<PipeDoc>> bufferCache = new ConcurrentHashMap<>();
    
    @Inject
    @ConfigProperty(name = "quarkus.application.name", defaultValue = "unknown-module")
    String applicationName;

    /**
     * Intercepts method calls annotated with @ProcessingBuffered.
     * 
     * @param context The invocation context containing method and parameters
     * @return The original method result, with buffer capture as a side effect
     * @throws Exception If the original method throws an exception
     */
    @AroundInvoke
    public Object captureProcessingOutput(InvocationContext context) throws Exception {
        ProcessingBuffered annotation = getProcessingBufferedAnnotation(context);
        if (annotation == null) {
            // No annotation found, proceed normally
            return context.proceed();
        }

        // Check if buffer capture is enabled
        boolean bufferEnabled = resolveConfigValue(annotation.enabled(), "processing.buffer.enabled", false);
        if (!bufferEnabled) {
            LOG.debugf("Processing buffer disabled for method: %s", context.getMethod().getName());
            return context.proceed();
        }

        // Get or create buffer for this method
        String methodKey = getMethodKey(context);
        ProcessingBuffer<PipeDoc> buffer = getOrCreateBuffer(methodKey, annotation);
        
        // Execute the original method
        Object result = context.proceed();
        
        // Capture output if it's a reactive result
        if (result instanceof Uni) {
            Uni<?> originalUni = (Uni<?>) result;
            return originalUni.onItem().transform(item -> {
                captureFromProcessResponse(item, buffer);
                return item;
            });
        } else {
            // Direct result - capture immediately
            captureFromProcessResponse(result, buffer);
            return result;
        }
    }

    /**
     * Gets the ProcessingBuffered annotation from the method or class.
     */
    private ProcessingBuffered getProcessingBufferedAnnotation(InvocationContext context) {
        // First check method-level annotation
        ProcessingBuffered methodAnnotation = context.getMethod().getAnnotation(ProcessingBuffered.class);
        if (methodAnnotation != null) {
            return methodAnnotation;
        }
        
        // Fall back to class-level annotation
        return context.getTarget().getClass().getAnnotation(ProcessingBuffered.class);
    }

    /**
     * Creates a unique key for caching buffers per method.
     */
    private String getMethodKey(InvocationContext context) {
        return context.getTarget().getClass().getSimpleName() + "." + context.getMethod().getName();
    }

    /**
     * Gets an existing buffer or creates a new one for the given method.
     */
    private ProcessingBuffer<PipeDoc> getOrCreateBuffer(String methodKey, ProcessingBuffered annotation) {
        return bufferCache.computeIfAbsent(methodKey, key -> {
            // Resolve configuration values
            int capacity = resolveConfigValue(annotation.capacity(), "processing.buffer.capacity", 100);
            String directory = resolveConfigValue(annotation.directory(), "processing.buffer.directory", "target/test-data");
            String prefix = resolveConfigValue(annotation.prefix(), "processing.buffer.prefix", applicationName);
            
            LOG.infof("Creating ProcessingBuffer for %s: capacity=%d, directory=%s, prefix=%s", 
                     key, capacity, directory, prefix);
            
            // Create buffer using the factory
            return ProcessingBufferFactory.createBuffer(true, capacity, PipeDoc.class);
        });
    }

    /**
     * Captures PipeDoc from ProcessResponse and adds it to the buffer.
     */
    private void captureFromProcessResponse(Object result, ProcessingBuffer<PipeDoc> buffer) {
        PipeDoc outputDoc = extractOutputDoc(result);
        if (outputDoc == null) {
            return;
        }

        buffer.add(outputDoc);
        LOG.debugf("Captured document in buffer: %s (buffer size: %d)",
                outputDoc.getDocId(), buffer.size());
    }

    /**
     * Extracts {@code output_doc} as a {@link PipeDoc} from a {@code ProcessDataResponse}.
     *
     * <p>Kept descriptor-based (no compile-time dependency on generated response types) and
     * JSON-free (uses protobuf binary parse).</p>
     *
     * <p>Package-private for unit testing without booting Quarkus.</p>
     */
    static PipeDoc extractOutputDoc(Object result) {
        // Keep this interceptor generic: do not depend on generated module RPC classes.
        // We detect the response type and output_doc field by descriptor name.
        if (!(result instanceof Message msg)) {
            return null;
        }

        Descriptors.Descriptor d = msg.getDescriptorForType();
        String fullName = d.getFullName();

        // Current module RPC return type (pipeline-module protos)
        if (!"ai.pipestream.data.module.v1.ProcessDataResponse".equals(fullName)) {
            return null;
        }

        Descriptors.FieldDescriptor fd = d.findFieldByName("output_doc");
        if (fd == null || !msg.hasField(fd)) {
            return null;
        }

        Object fieldValue = msg.getField(fd);
        if (!(fieldValue instanceof Message outputMsg)) {
            return null;
        }

        try {
            if (outputMsg instanceof PipeDoc) {
                return (PipeDoc) outputMsg;
            }
            // DynamicMessage (or other Message) -> parse into concrete PipeDoc without JSON
            return PipeDoc.parseFrom(outputMsg.toByteString());
        } catch (Exception e) {
            LOG.debugf(e, "Failed to parse output_doc into PipeDoc for response type %s", fullName);
            return null;
        }
    }

    /**
     * Resolves configuration values from annotation parameters or system configuration.
     * Supports config expressions like "${property.name:defaultValue}".
     */
    private <T> T resolveConfigValue(String annotationValue, String configProperty, T defaultValue) {
        // If annotation value is empty, use config property
        if (annotationValue == null || annotationValue.trim().isEmpty()) {
            return getConfigValue(configProperty, defaultValue);
        }
        
        // Handle config expressions like "${property:default}"
        if (annotationValue.startsWith("${") && annotationValue.endsWith("}")) {
            String expression = annotationValue.substring(2, annotationValue.length() - 1);
            String[] parts = expression.split(":", 2);
            String propertyName = parts[0];
            T fallbackDefault = parts.length > 1 ? parseValue(parts[1], defaultValue) : defaultValue;
            return getConfigValue(propertyName, fallbackDefault);
        }
        
        // Direct value
        return parseValue(annotationValue, defaultValue);
    }

    /**
     * Gets a configuration value with fallback to default.
     */
    @SuppressWarnings("unchecked")
    private <T> T getConfigValue(String propertyName, T defaultValue) {
        try {
            if (defaultValue instanceof Boolean) {
                return (T) Boolean.valueOf(System.getProperty(propertyName, defaultValue.toString()));
            } else if (defaultValue instanceof Integer) {
                return (T) Integer.valueOf(System.getProperty(propertyName, defaultValue.toString()));
            } else if (defaultValue instanceof String) {
                return (T) System.getProperty(propertyName, (String) defaultValue);
            }
        } catch (Exception e) {
            LOG.warnf("Failed to parse config property %s, using default: %s", propertyName, defaultValue);
        }
        return defaultValue;
    }

    /**
     * Parses a string value to the target type.
     */
    @SuppressWarnings("unchecked")
    private <T> T parseValue(String value, T defaultValue) {
        try {
            if (defaultValue instanceof Boolean) {
                return (T) Boolean.valueOf(value);
            } else if (defaultValue instanceof Integer) {
                return (T) Integer.valueOf(value);
            } else if (defaultValue instanceof String) {
                return (T) value;
            }
        } catch (Exception e) {
            LOG.warnf("Failed to parse value '%s', using default: %s", value, defaultValue);
        }
        return defaultValue;
    }

    // ============= Static Buffer Access Methods (Quarkus Pattern) =============

    /**
     * Gets the buffer for a specific method, if it exists.
     * 
     * @param methodKey The method key in format "ClassName.methodName"
     * @return The buffer for the method, or null if not found
     */
    public static ProcessingBuffer<PipeDoc> getBuffer(String methodKey) {
        return bufferCache.get(methodKey);
    }

    /**
     * Gets the buffer for a specific class and method name.
     * 
     * @param className The simple class name
     * @param methodName The method name
     * @return The buffer for the method, or null if not found
     */
    public static ProcessingBuffer<PipeDoc> getBuffer(String className, String methodName) {
        return getBuffer(className + "." + methodName);
    }

    /**
     * Gets all active buffer keys (method identifiers).
     * 
     * @return Set of all method keys that have active buffers
     */
    public static java.util.Set<String> getActiveBufferKeys() {
        return new java.util.HashSet<>(bufferCache.keySet());
    }

    /**
     * Gets the total number of documents across all buffers.
     * 
     * @return Total count of captured documents
     */
    public static int getTotalCapturedDocuments() {
        return bufferCache.values().stream()
                .mapToInt(ProcessingBuffer::size)
                .sum();
    }

    /**
     * Saves all buffers to disk using their configured directories and prefixes.
     * This is a future trigger capability for batch saving.
     */
    public static void saveAllBuffers() {
        LOG.info("Saving all active buffers to disk...");
        for (java.util.Map.Entry<String, ProcessingBuffer<PipeDoc>> entry : bufferCache.entrySet()) {
            try {
                ProcessingBuffer<PipeDoc> buffer = entry.getValue();
                if (buffer.size() > 0) {
                    // Use default save parameters - could be enhanced with per-buffer config
                    buffer.saveToDisk();
                    LOG.infof("Saved buffer %s: %d documents", entry.getKey(), buffer.size());
                }
            } catch (Exception e) {
                LOG.errorf(e, "Failed to save buffer %s", entry.getKey());
            }
        }
        LOG.info("Completed saving all buffers");
    }

    /**
     * Saves a specific buffer to disk.
     * 
     * @param methodKey The method key identifying the buffer
     * @return true if buffer was found and saved, false otherwise
     */
    public static boolean saveBuffer(String methodKey) {
        ProcessingBuffer<PipeDoc> buffer = bufferCache.get(methodKey);
        if (buffer != null && buffer.size() > 0) {
            try {
                buffer.saveToDisk();
                LOG.infof("Saved buffer %s: %d documents", methodKey, buffer.size());
                return true;
            } catch (Exception e) {
                LOG.errorf(e, "Failed to save buffer %s", methodKey);
            }
        }
        return false;
    }

    /**
     * Clears all buffers.
     * Useful for testing or memory management.
     */
    public static void clearAllBuffers() {
        LOG.info("Clearing all active buffers...");
        bufferCache.values().forEach(ProcessingBuffer::clear);
        LOG.infof("Cleared %d buffers", bufferCache.size());
    }

    /**
     * Gets buffer statistics for monitoring.
     * 
     * @return Map of method keys to buffer sizes
     */
    public static java.util.Map<String, Integer> getBufferStatistics() {
        return bufferCache.entrySet().stream()
                .collect(Collectors.toMap(
                        java.util.Map.Entry::getKey,
                        entry -> entry.getValue().size()
                ));
    }
}