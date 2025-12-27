package ai.pipestream.engine.mapping.util.service;

import io.quarkus.smallrye.openapi.runtime.OpenApiDocumentService;
import io.smallrye.openapi.runtime.io.Format;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.json.*;
import org.jboss.logging.Logger;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Service for extracting OpenAPI schema components from the dynamically generated OpenAPI document.
 * This service provides a centralized way to access schema definitions for validation and registration.
 * <p>
 * Pattern established for chunker module and designed to be reused across all modules and services.
 */
@ApplicationScoped
public class SchemaExtractorService {

    private static final Logger LOG = Logger.getLogger(SchemaExtractorService.class);

    private final Instance<OpenApiDocumentService> openApiDocumentService;

    @Inject
    public SchemaExtractorService(Instance<OpenApiDocumentService> openApiDocumentService) {
        this.openApiDocumentService = openApiDocumentService;
    }

    /**
     * Extracts a specific schema component from the OpenAPI document by name.
     * 
     * @param schemaName The name of the schema to extract (e.g., "ChunkerConfig", "ParserConfig")
     * @return Optional containing the schema as JSON string if found, empty if not found
     */
    public Optional<String> extractSchemaByName(String schemaName) {
        LOG.debugf("Extracting schema for: %s", schemaName);
        
        try {
            OpenApiDocumentService documentService = openApiDocumentService.isResolvable()
                    ? openApiDocumentService.get()
                    : null;
            
            if (documentService == null) {
                LOG.warnf("OpenApiDocumentService not available - schema extraction failed for: %s", schemaName);
                return Optional.empty();
            }
            
            // Get the full OpenAPI document as JSON
            byte[] jsonBytes = documentService.getDocument(Format.JSON);
            if (jsonBytes == null || jsonBytes.length == 0) {
                LOG.warnf("OpenAPI document is empty - schema extraction failed for: %s", schemaName);
                return Optional.empty();
            }
            
            String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);
            LOG.debugf("OpenAPI document size: %d characters", jsonString.length());
            
            // Parse the JSON and extract the specific schema
            try (JsonReader reader = Json.createReader(new StringReader(jsonString))) {
                JsonObject openApiDoc = reader.readObject();
                
                // Navigate to components.schemas.schemaName
                JsonObject components = openApiDoc.getJsonObject("components");
                if (components == null) {
                    LOG.debugf("No components section found in OpenAPI document for schema: %s", schemaName);
                    return Optional.empty();
                }
                
                JsonObject schemas = components.getJsonObject("schemas");
                if (schemas == null) {
                    LOG.debugf("No schemas section found in OpenAPI components for schema: %s", schemaName);
                    return Optional.empty();
                }
                
                JsonObject targetSchema = schemas.getJsonObject(schemaName);
                if (targetSchema == null) {
                    LOG.debugf("Schema '%s' not found in OpenAPI schemas section", schemaName);
                    return Optional.empty();
                }
                
                String extractedSchema = targetSchema.toString();
                LOG.debugf("Successfully extracted schema '%s' (%d characters)", schemaName, extractedSchema.length());
                
                return Optional.of(extractedSchema);
            }
            
        } catch (Exception e) {
            LOG.errorf(e, "Error extracting schema '%s' from OpenAPI document", schemaName);
            return Optional.empty();
        }
    }

    /**
     * Convenience method to extract the ChunkerConfig schema specifically.
     * This is the proven pattern from our test cases.
     * 
     * @return Optional containing ChunkerConfig schema as JSON string if found
     */
    public Optional<String> extractChunkerConfigSchema() {
        return extractSchemaByName("ChunkerConfig");
    }

    /**
     * Extracts ChunkerConfig schema and cleans it for JSON Schema v7 validation.
     * Removes OpenAPI-specific elements like $ref and x-hidden extensions.
     * 
     * @return Optional containing JSON Schema v7 compatible schema
     */
    public Optional<String> extractChunkerConfigSchemaForValidation() {
        return extractSchemaByName("ChunkerConfig").map(this::cleanSchemaForJsonSchemaV7);
    }

    /**
     * Convenience method to extract the ParserConfig schema specifically.
     * 
     * @return Optional containing ParserConfig schema as JSON string if found
     */
    public Optional<String> extractParserConfigSchema() {
        return extractSchemaByName("ParserConfig");
    }

    /**
     * Extracts ParserConfig schema and cleans it for JSON Schema v7 validation.
     * Removes OpenAPI-specific elements like $ref and x-hidden extensions.
     * 
     * @return Optional containing JSON Schema v7 compatible schema
     */
    public Optional<String> extractParserConfigSchemaForValidation() {
        return extractSchemaByName("ParserConfig").map(this::cleanSchemaForJsonSchemaV7);
    }

    /**
     * Extracts a schema and resolves OpenAPI $ref references against components.schemas,
     * returning a JSON Schema object suitable for JSONForms rendering (no x-* extensions,
     * inline referenced schemas).
     */
    public Optional<String> extractSchemaResolvedForJsonForms(String schemaName) {
        LOG.debugf("Extracting JSONForms-ready schema for: %s", schemaName);
        try {
            OpenApiDocumentService documentService = openApiDocumentService.isResolvable()
                    ? openApiDocumentService.get()
                    : null;
            if (documentService == null) {
                LOG.warn("OpenApiDocumentService not available - cannot resolve refs");
                return Optional.empty();
            }

            byte[] jsonBytes = documentService.getDocument(Format.JSON);
            if (jsonBytes == null || jsonBytes.length == 0) {
                LOG.warn("OpenAPI document is empty - cannot resolve refs");
                return Optional.empty();
            }

            String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);
            try (JsonReader reader = Json.createReader(new StringReader(jsonString))) {
                JsonObject openApiDoc = reader.readObject();
                JsonObject components = openApiDoc.getJsonObject("components");
                if (components == null) return Optional.empty();
                JsonObject schemas = components.getJsonObject("schemas");
                if (schemas == null) return Optional.empty();
                JsonObject target = schemas.getJsonObject(schemaName);
                if (target == null) return Optional.empty();

                JsonObject resolved = resolveRefsAndClean(target, schemas);
                // Ensure draft-07 meta for validators like Apicurio JSON rules and JSONForms tooling
                JsonObject withMeta = addDraft7MetaIfMissing(resolved);
                String resolvedString = withMeta.toString();
                LOG.debugf("Resolved schema '%s' for JSONForms (%d chars)", schemaName, resolvedString.length());
                return Optional.of(resolvedString);
            }
        } catch (Exception e) {
            LOG.errorf(e, "Error resolving schema '%s' for JSONForms", schemaName);
            return Optional.empty();
        }
    }

    /**
     * Convenience method for ParserConfig specifically.
     */
    public Optional<String> extractParserConfigSchemaResolvedForJsonForms() {
        return extractSchemaResolvedForJsonForms("ParserConfig");
    }

    /**
     * Resolves $ref that point to #/components/schemas/... and removes x-* keys.
     * If a node contains a $ref alongside other properties, the referenced object is
     * used as a base and the other properties are overlaid on top (recursively processed).
     */
    private JsonObject resolveRefsAndClean(JsonObject node, JsonObject componentsSchemas) {
        // If node has a $ref, resolve first and then overlay remaining keys
        if (node.containsKey("$ref")) {
            String ref = node.getString("$ref", null);
            JsonObject base = resolveRef(ref, componentsSchemas).orElse(Json.createObjectBuilder().build());
            JsonObject resolvedBase = resolveRefsAndClean(base, componentsSchemas);

            var builder = Json.createObjectBuilder();
            // Copy base first
            resolvedBase.forEach(builder::add);

            // Overlay remaining keys from the original node (except $ref and x-*)
            for (String key : node.keySet()) {
                if ("$ref".equals(key) || key.startsWith("x-")) continue;
                var value = node.get(key);
                if (value instanceof JsonObject) {
                    builder.add(key, resolveRefsAndClean((JsonObject) value, componentsSchemas));
                } else if (value instanceof JsonArray) {
                    builder.add(key, resolveArray((JsonArray) value, componentsSchemas));
                } else {
                    builder.add(key, value);
                }
            }
            return builder.build();
        }

        // Otherwise, recursively process children and strip x-* keys
        var builder = Json.createObjectBuilder();
        for (String key : node.keySet()) {
            if (key.startsWith("x-")) continue;
            var value = node.get(key);
            if (value instanceof JsonObject) {
                builder.add(key, resolveRefsAndClean((JsonObject) value, componentsSchemas));
            } else if (value instanceof JsonArray) {
                builder.add(key, resolveArray((JsonArray) value, componentsSchemas));
            } else {
                builder.add(key, value);
            }
        }
        return builder.build();
    }

    private JsonArray resolveArray(JsonArray array, JsonObject componentsSchemas) {
        JsonArrayBuilder ab = Json.createArrayBuilder();
        for (var v : array) {
            if (v instanceof JsonObject) {
                ab.add(resolveRefsAndClean((JsonObject) v, componentsSchemas));
            } else {
                ab.add(v);
            }
        }
        return ab.build();
    }

    private Optional<JsonObject> resolveRef(String ref, JsonObject componentsSchemas) {
        if (ref == null) return Optional.empty();
        // Only handle internal component refs of the form #/components/schemas/Name
        final String prefix = "#/components/schemas/";
        if (ref.startsWith(prefix)) {
            String name = ref.substring(prefix.length());
            JsonObject target = componentsSchemas.getJsonObject(name);
            if (target != null) return Optional.of(target);
        }
        LOG.debugf("Unsupported or unresolved $ref: %s", ref);
        return Optional.empty();
    }

    /**
     * Adds a draft-07 $schema meta if missing.
     */
    private JsonObject addDraft7MetaIfMissing(JsonObject obj) {
        if (obj.containsKey("$schema")) return obj;
        var builder = Json.createObjectBuilder();
        builder.add("$schema", "http://json-schema.org/draft-07/schema#");
        obj.forEach(builder::add);
        return builder.build();
    }

    /**
     * Cleans an OpenAPI schema to make it compatible with JSON Schema v7 validation.
     * Removes $ref references and x-* extensions that aren't valid in JSON Schema.
     */
    private String cleanSchemaForJsonSchemaV7(String openApiSchema) {
        try (JsonReader reader = Json.createReader(new StringReader(openApiSchema))) {
            JsonObject schema = reader.readObject();
            return cleanJsonObjectForValidation(schema).toString();
        } catch (Exception e) {
            LOG.warnf(e, "Failed to clean schema for JSON Schema v7 validation, returning original");
            return openApiSchema;
        }
    }

    /**
     * Recursively cleans a JsonObject by removing OpenAPI-specific elements.
     */
    private JsonObject cleanJsonObjectForValidation(JsonObject obj) {
        var builder = Json.createObjectBuilder();
        
        obj.forEach((key, value) -> {
            // Skip OpenAPI-specific extensions
            if (key.startsWith("x-")) {
                return;
            }
            
            // Skip $ref as we want inline schemas for validation
            if ("$ref".equals(key)) {
                return;
            }
            
            // Recursively clean nested objects and arrays
            if (value instanceof JsonObject) {
                builder.add(key, cleanJsonObjectForValidation((JsonObject) value));
            } else if (value instanceof JsonArray) {
                JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
                ((JsonArray) value).forEach(arrayValue -> {
                    if (arrayValue instanceof JsonObject) {
                        arrayBuilder.add(cleanJsonObjectForValidation((JsonObject) arrayValue));
                    } else {
                        arrayBuilder.add(arrayValue);
                    }
                });
                builder.add(key, arrayBuilder.build());
            } else {
                builder.add(key, value);
            }
        });
        
        return builder.build();
    }

    /**
     * Convenience method to extract any config schema by inferring the name from a class.
     * Assumes the OpenAPI schema name matches the simple class name.
     * 
     * @param configClass The configuration class (e.g., ChunkerConfig.class)
     * @return Optional containing the schema as JSON string if found
     */
    public Optional<String> extractSchemaForClass(Class<?> configClass) {
        String schemaName = configClass.getSimpleName();
        return extractSchemaByName(schemaName);
    }

    /**
     * Validates that the OpenAPI document service is available and functional.
     * Useful for health checks and startup validation.
     * 
     * @return true if the service is available and can generate an OpenAPI document
     */
    public boolean isOpenApiDocumentAvailable() {
        try {
            OpenApiDocumentService documentService = openApiDocumentService.isResolvable()
                    ? openApiDocumentService.get()
                    : null;
            
            if (documentService == null) {
                return false;
            }
            
            byte[] jsonBytes = documentService.getDocument(Format.JSON);
            return jsonBytes != null && jsonBytes.length > 0;
            
        } catch (Exception e) {
            LOG.debugf(e, "OpenAPI document availability check failed");
            return false;
        }
    }

    /**
     * Gets the full OpenAPI document as a JSON string.
     * Useful for debugging or providing the complete API specification.
     * 
     * @return Optional containing the full OpenAPI document as JSON string
     */
    public Optional<String> getFullOpenApiDocument() {
        try {
            OpenApiDocumentService documentService = openApiDocumentService.isResolvable()
                    ? openApiDocumentService.get()
                    : null;
            
            if (documentService == null) {
                return Optional.empty();
            }
            
            byte[] jsonBytes = documentService.getDocument(Format.JSON);
            if (jsonBytes == null || jsonBytes.length == 0) {
                return Optional.empty();
            }
            
            return Optional.of(new String(jsonBytes, StandardCharsets.UTF_8));
            
        } catch (Exception e) {
            LOG.errorf(e, "Error getting full OpenAPI document");
            return Optional.empty();
        }
    }
}
