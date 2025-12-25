# Mapping & Filtering

## Overview

Mapping and filtering happen **inside the engine** to avoid network hops. Since these are lightweight operations (field copies, CEL evaluations), there's no need for the latency of remote calls.

```
┌─────────────────────────────────────────────────────────────────┐
│                         ENGINE                                   │
│                                                                  │
│   PipeStream ──▶ [Filter] ──▶ [Pre-Map] ──▶ Module ──▶ [Post-Map]│
│       │             │            │                        │      │
│       │             │            │                        │      │
│       │          CEL eval     CEL transforms           CEL transforms
│       │          (skip?)      (prepare input)          (normalize output)
│       │                                                          │
│       └──────────────────────────────────────────────────────────┘
│                        All in-JVM, no network                    │
└─────────────────────────────────────────────────────────────────┘
```

## Node Filter (CEL)

Each node can have a filter condition. If it evaluates to `false`, the node is skipped entirely:

```protobuf
message NodeProcessingConfig {
  string node_id = 1;
  
  // CEL expression - if false, skip this node
  optional string filter_condition = 6;
  
  repeated ProcessingMapping pre_mappings = 2;
  repeated ProcessingMapping post_mappings = 3;
  // ...
}
```

**Example filters:**

```cel
// Only process PDFs
has(doc.blob_bag.blob) && doc.blob_bag.blob.mime_type == "application/pdf"

// Only process if not already chunked
!has(doc.search_metadata.semantic_results) || 
 doc.search_metadata.semantic_results.semantic_chunks.size() == 0

// Only process English documents
doc.search_metadata.language == "en"
```

## Field Mappings

Mappings copy/transform fields before and after module processing:

```protobuf
message ProcessingMapping {
  string mapping_id = 1;
  repeated string source_field_paths = 2;  // Proto field paths
  repeated string target_field_paths = 3;
  MappingType mapping_type = 4;
  
  oneof mapping_config {
    TransformConfig transform_config = 5;
    AggregateConfig aggregate_config = 6;
    SplitConfig split_config = 7;
  }
}

enum MappingType {
  MAPPING_TYPE_UNSPECIFIED = 0;
  MAPPING_TYPE_DIRECT = 1;      // Copy field as-is
  MAPPING_TYPE_TRANSFORM = 2;   // Apply CEL transformation
  MAPPING_TYPE_AGGREGATE = 3;   // Combine multiple fields
  MAPPING_TYPE_SPLIT = 4;       // Split one field to many
}
```

## Transform Config (CEL)

```protobuf
message TransformConfig {
  // CEL expression returning the transformed value
  // Variables: `value` (source field), `doc` (full PipeDoc)
  string cel_expression = 3;
  
  // Deprecated - use cel_expression
  string rule_name = 1 [deprecated = true];
  google.protobuf.Struct params = 2 [deprecated = true];
}
```

**Example transforms:**

```cel
// Uppercase
value.upperAscii()

// Trim whitespace
value.trim()

// Substring
value.substring(0, 100)

// Default value
value.size() > 0 ? value : "untitled"

// Conditional based on other fields
doc.search_metadata.language == "en" ? value : translate(value, "en")
```

## Mapping Engine Implementation

```java
public class MappingEngine {
    private final CelEvaluator celEvaluator;
    private final FieldResolver fieldResolver;
    
    public PipeDoc applyMappings(PipeDoc doc, List<ProcessingMapping> mappings) {
        PipeDoc.Builder builder = doc.toBuilder();
        
        for (ProcessingMapping mapping : mappings) {
            Object sourceValue = getSourceValue(doc, mapping);
            Object targetValue = transformValue(sourceValue, doc, mapping);
            setTargetValue(builder, mapping.getTargetFieldPathsList(), targetValue);
        }
        
        return builder.build();
    }
    
    private Object getSourceValue(PipeDoc doc, ProcessingMapping mapping) {
        switch (mapping.getMappingType()) {
            case DIRECT:
            case TRANSFORM:
                // Single source field
                return fieldResolver.getValue(doc, mapping.getSourceFieldPaths(0));
                
            case AGGREGATE:
                // Multiple source fields
                return mapping.getSourceFieldPathsList().stream()
                    .map(path -> fieldResolver.getValue(doc, path))
                    .collect(Collectors.toList());
                
            case SPLIT:
                return fieldResolver.getValue(doc, mapping.getSourceFieldPaths(0));
                
            default:
                throw new IllegalArgumentException("Unknown mapping type");
        }
    }
    
    private Object transformValue(Object value, PipeDoc doc, ProcessingMapping mapping) {
        switch (mapping.getMappingType()) {
            case DIRECT:
                return value;
                
            case TRANSFORM:
                String expr = mapping.getTransformConfig().getCelExpression();
                return celEvaluator.evaluate(expr, Map.of(
                    "value", value,
                    "doc", doc
                ));
                
            case AGGREGATE:
                List<?> values = (List<?>) value;
                AggregateConfig config = mapping.getAggregateConfig();
                if (config.getAggregationType() == CONCATENATE) {
                    return values.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(config.getDelimiter()));
                }
                // ... other aggregations
                
            case SPLIT:
                String delimiter = mapping.getSplitConfig().getDelimiter();
                return Arrays.asList(value.toString().split(delimiter));
                
            default:
                return value;
        }
    }
}
```

## OOTB Mappings by Module Capability

The frontend can suggest default mappings based on module capability:

| Capability | Default Post-Mappings |
|------------|-----------------------|
| **PARSER** | `parsed_metadata.{parser}.data.content` → `search_metadata.body` |
| **CHUNKER** | Output already in `semantic_results` |
| **EMBEDDER** | Output already in `semantic_results[].embedding_info` |
| **ENRICHER** | Custom - user configures target fields |

```java
List<ProcessingMapping> getDefaultPostMappings(ModuleCapability capability, String parserType) {
    switch (capability) {
        case PARSER:
            return List.of(
                // Map parsed content to searchable body
                ProcessingMapping.newBuilder()
                    .setMappingId("parser-body")
                    .addSourceFieldPaths("parsed_metadata." + parserType + ".data.content")
                    .addTargetFieldPaths("search_metadata.body")
                    .setMappingType(DIRECT)
                    .build(),
                // Map parsed title
                ProcessingMapping.newBuilder()
                    .setMappingId("parser-title")
                    .addSourceFieldPaths("parsed_metadata." + parserType + ".data.title")
                    .addTargetFieldPaths("search_metadata.title")
                    .setMappingType(DIRECT)
                    .build()
            );
        default:
            return List.of();
    }
}
```

## Field Path Resolution

Field paths use proto field names with dot notation:

```
search_metadata.body
search_metadata.tags.tag_data[0]
parsed_metadata.tika.data.content
blob_bag.blob.size_bytes
structured_data.fields["custom_field"]
```

The `FieldResolver` uses proto descriptors for type-safe access:

```java
public class FieldResolver {
    public Object getValue(Message message, String path) {
        String[] parts = path.split("\\.");
        Object current = message;
        
        for (String part : parts) {
            if (current instanceof Message) {
                Descriptors.FieldDescriptor field = 
                    ((Message) current).getDescriptorForType().findFieldByName(part);
                current = ((Message) current).getField(field);
            } else if (current instanceof Map) {
                current = ((Map<?, ?>) current).get(part);
            }
            // Handle array indexing, etc.
        }
        
        return current;
    }
}
```
