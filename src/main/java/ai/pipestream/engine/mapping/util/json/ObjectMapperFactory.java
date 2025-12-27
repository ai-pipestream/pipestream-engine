
package ai.pipestream.engine.mapping.util.json;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/**
 * Factory for creating consistently configured Jackson {@link ObjectMapper} instances.
 * <p>
 * Ensures all JSON serialization/deserialization across the pipeline uses the same configuration.
 * </p>
 * <h2>Configuration highlights</h2>
 * <ul>
 *   <li>Alphabetical property sorting ({@link MapperFeature#SORT_PROPERTIES_ALPHABETICALLY})</li>
 *   <li>Map entries ordered by keys ({@link SerializationFeature#ORDER_MAP_ENTRIES_BY_KEYS})</li>
 *   <li>camelCase property naming strategy ({@link PropertyNamingStrategies#LOWER_CAMEL_CASE})</li>
 *   <li>ISO-8601 date/time formatting (no numeric timestamps)</li>
 *   <li>Java 8 date/time support via {@link JavaTimeModule}</li>
 *   <li>Do not fail on unknown properties during deserialization
 *       ({@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES})</li>
 * </ul>
 * <p>
 * Provides a CDI-produced singleton {@link ObjectMapper} and static factory helpers.
 * </p>
 *
 * @see JsonMapper
 * @see JavaTimeModule
 */
@Singleton
public class ObjectMapperFactory {

    // Static final field for use in static contexts
    private static final ObjectMapper STATIC_CONFIGURED_MAPPER = createStaticConfiguredMapper();
    
    // Default constructor for CDI
    public ObjectMapperFactory() {
    }

    /**
     * Creates a fully configured ObjectMapper with all standard settings.
     * This is the primary method that should be used throughout the application.
     * Returns the cached static instance for better performance.
     */
    public static ObjectMapper createConfiguredMapper() {
        return STATIC_CONFIGURED_MAPPER;
    }
    
    /**
     * Private method for static initialization of the configured mapper.
     */
    private static ObjectMapper createStaticConfiguredMapper() {
        return applyConfigurationToBuilder(JsonMapper.builder()).build();
    }
    
    /**
     * Bean producer method for CDI injection.
     * This allows the ObjectMapper to be injected where needed.
     */
    @Singleton
    @Produces
    public ObjectMapper produceObjectMapper() {
        return createConfiguredMapper();
    }

    /**
     * Creates a minimal ObjectMapper without ordering or special naming.
     * Use this only when you need to match external JSON formats exactly.
     */
    public static ObjectMapper createMinimalMapper() {
        return JsonMapper.builder()
                // Don't fail on unknown properties
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

                // Add Java 8 time support
                .addModule(new JavaTimeModule())

                .build();
    }


    /**
     * Private helper method to apply standard configuration to a JsonMapper.Builder.
     * This ensures the same configuration is used for both builder and direct mapper approaches.
     */
    private static JsonMapper.Builder applyConfigurationToBuilder(JsonMapper.Builder builder) {
        return builder
                // Sort properties alphabetically for consistent output
                .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)

                // Use camelCase naming strategy (data scientist can handle it - industry standard for javascript)
                .propertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE)

                // Write dates as ISO-8601 strings, not timestamps
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false)

                // Don't fail on unknown properties (forward compatibility)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

                // Add Java 8 time support
                .addModule(new JavaTimeModule());
    }
}