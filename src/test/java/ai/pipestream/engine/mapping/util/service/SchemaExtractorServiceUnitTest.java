package ai.pipestream.engine.mapping.util.service;

import io.quarkus.smallrye.openapi.runtime.OpenApiDocumentService;
import io.smallrye.openapi.runtime.io.Format;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SchemaExtractorServiceUnitTest {

    @Test
    void resolvesJsonFormsSchemaWithoutQuarkusBoot() {
        // Mock OpenApiDocumentService + Instance wrapper so we don't need @QuarkusTest for coverage.
        OpenApiDocumentService documentService = mock(OpenApiDocumentService.class);
        @SuppressWarnings("unchecked")
        Instance<OpenApiDocumentService> instance = mock(Instance.class);
        when(instance.isResolvable()).thenReturn(true);
        when(instance.get()).thenReturn(documentService);

        String json = """
            {
              "openapi": "3.0.3",
              "components": {
                "schemas": {
                  "Inner": {
                    "type": "object",
                    "properties": {
                      "x-hidden": { "type": "string" },
                      "value": { "type": "string" }
                    }
                  },
                  "ParserConfig": {
                    "$ref": "#/components/schemas/Inner",
                    "x-internal": true
                  }
                }
              }
            }
            """;
        when(documentService.getDocument(Format.JSON)).thenReturn(json.getBytes(StandardCharsets.UTF_8));

        SchemaExtractorService svc = new SchemaExtractorService(instance);
        var resolved = svc.extractSchemaResolvedForJsonForms("ParserConfig");
        assertTrue(resolved.isPresent());

        String resolvedJson = resolved.get();
        assertFalse(resolvedJson.contains("x-hidden"));
        assertFalse(resolvedJson.contains("x-internal"));
        assertTrue(resolvedJson.contains("\"value\""));
    }
}

