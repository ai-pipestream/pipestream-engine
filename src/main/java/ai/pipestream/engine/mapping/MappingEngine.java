package ai.pipestream.engine.mapping;

import ai.pipestream.data.v1.AggregateConfig;
import ai.pipestream.data.v1.CelConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.ProcessingMapping;
import ai.pipestream.data.v1.TransformConfig;
import ai.pipestream.engine.mapping.util.ProtoFieldMapperImpl;
import ai.pipestream.engine.routing.CelEvaluatorService;
import com.google.protobuf.Value;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Core mapping engine for applying field mappings to PipeDoc documents.
 * <p>
 * This is an {@code @ApplicationScoped} CDI bean that can be injected into any service
 * that needs to apply {@link ProcessingMapping} transformations. It supports all mapping types:
 * DIRECT, TRANSFORM, AGGREGATE, SPLIT, and CEL.
 * <p>
 * Used by both {@link MappingServiceImpl} (gRPC service) and {@code EngineV1Service}
 * for pre/post mapping application.
 */
@ApplicationScoped
public class MappingEngine {

    private static final Logger LOG = Logger.getLogger(MappingEngine.class);

    private final ProtoFieldMapperImpl protoFieldMapper;
    private final CelEvaluatorService celEvaluatorService;

    /**
     * Default constructor for non-CDI usage (e.g., plain unit tests).
     */
    public MappingEngine() {
        this.protoFieldMapper = new ProtoFieldMapperImpl();
        this.celEvaluatorService = new CelEvaluatorService();
        this.celEvaluatorService.init();
    }

    /**
     * CDI constructor.
     */
    @Inject
    public MappingEngine(ProtoFieldMapperImpl protoFieldMapper, CelEvaluatorService celEvaluatorService) {
        this.protoFieldMapper = protoFieldMapper;
        this.celEvaluatorService = celEvaluatorService;
    }

    /**
     * Applies a list of ProcessingMappings to a PipeDoc.
     * <p>
     * This is the primary API used by EngineV1Service to apply pre/post mappings
     * from GraphNode configuration. Each mapping is applied in order.
     * <p>
     * Note: This differs from the gRPC MappingService which uses MappingRule with
     * candidate fallback semantics. Here, all mappings are applied sequentially.
     *
     * @param doc The document to transform
     * @param mappings The list of mappings to apply
     * @param stream The PipeStream context (used for CEL evaluation)
     * @return A Uni that completes with the transformed document
     */
    public Uni<PipeDoc> applyMappings(PipeDoc doc, List<ProcessingMapping> mappings, PipeStream stream) {
        if (mappings == null || mappings.isEmpty()) {
            return Uni.createFrom().item(doc);
        }

        PipeDoc.Builder docBuilder = doc.toBuilder();
        for (ProcessingMapping mapping : mappings) {
            // Update stream context with current doc state for each mapping
            PipeStream currentContext = stream.toBuilder()
                    .setDocument(docBuilder.build())
                    .build();
            applyMapping(docBuilder, mapping, currentContext);
        }
        return Uni.createFrom().item(docBuilder.build());
    }

    /**
     * Applies a single ProcessingMapping to a document builder.
     * <p>
     * Returns true if the mapping was applied successfully, false otherwise.
     * This return value is used by MappingServiceImpl for candidate fallback logic.
     *
     * @param docBuilder The document builder to modify
     * @param mapping The mapping to apply
     * @param stream The PipeStream context for CEL evaluation (can be null for non-CEL mappings)
     * @return true if mapping succeeded, false otherwise
     */
    public boolean applyMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping, PipeStream stream) {
        boolean success = false;
        switch (mapping.getMappingType()) {
            case MAPPING_TYPE_DIRECT:
                success = handleDirectMapping(docBuilder, mapping);
                break;
            case MAPPING_TYPE_TRANSFORM:
                success = handleTransformMapping(docBuilder, mapping);
                break;
            case MAPPING_TYPE_AGGREGATE:
                success = handleAggregateMapping(docBuilder, mapping);
                break;
            case MAPPING_TYPE_SPLIT:
                success = handleSplitMapping(docBuilder, mapping);
                break;
            case MAPPING_TYPE_CEL:
                success = handleCelMapping(docBuilder, mapping, stream);
                break;
            case MAPPING_TYPE_UNSPECIFIED:
            default:
                LOG.debugf("Skipping unspecified mapping type for mapping_id=%s", mapping.getMappingId());
                break;
        }
        if (!success) {
            LOG.debugf("Mapping %s (type=%s) did not apply successfully",
                    mapping.getMappingId(), mapping.getMappingType());
        }
        return success;
    }

    // ========== Individual mapping handlers ==========

    private boolean handleCelMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping, PipeStream stream) {
        if (!mapping.hasCelConfig() || mapping.getTargetFieldPathsCount() == 0) {
            return false;
        }

        CelConfig config = mapping.getCelConfig();
        String targetPath = mapping.getTargetFieldPaths(0);

        // Build context for CEL evaluation
        PipeStream context;
        if (stream != null) {
            context = stream.toBuilder().setDocument(docBuilder.build()).build();
        } else {
            context = PipeStream.newBuilder().setDocument(docBuilder.build()).build();
        }

        Object result = celEvaluatorService.evaluateValue(config.getExpression(), context);

        if (result != null) {
            setRawValue(docBuilder, targetPath, result);
            return true;
        }

        return false;
    }

    private boolean handleDirectMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping) {
        if (mapping.getSourceFieldPathsCount() == 0 || mapping.getTargetFieldPathsCount() == 0) {
            return false; // Invalid DIRECT mapping
        }

        String sourcePath = mapping.getSourceFieldPaths(0);
        String targetPath = mapping.getTargetFieldPaths(0);

        Optional<Object> sourceValue = getRawValue(docBuilder, sourcePath);

        if (sourceValue.isPresent()) {
            setRawValue(docBuilder, targetPath, sourceValue.get());
            return true;
        }

        return false;
    }

    private boolean handleAggregateMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping) {
        if (mapping.getSourceFieldPathsCount() < 2 || mapping.getTargetFieldPathsCount() != 1) {
            return false; // Invalid AGGREGATE mapping
        }

        AggregateConfig config = mapping.getAggregateConfig();
        String targetPath = mapping.getTargetFieldPaths(0);

        switch (config.getAggregationType()) {
            case AGGREGATION_TYPE_CONCATENATE:
                return handleConcatenate(docBuilder, mapping.getSourceFieldPathsList(), targetPath, config.getDelimiter());
            case AGGREGATION_TYPE_SUM:
                return handleSum(docBuilder, mapping.getSourceFieldPathsList(), targetPath);
            default:
                return false;
        }
    }

    private boolean handleConcatenate(PipeDoc.Builder docBuilder, List<String> sourcePaths, String targetPath, String delimiter) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (String path : sourcePaths) {
            Optional<Object> value = getRawValue(docBuilder, path);
            if (value.isEmpty() || !(value.get() instanceof String)) {
                return false; // A source field was missing or not a string
            }
            if (!first) {
                result.append(delimiter);
            }
            result.append((String) value.get());
            first = false;
        }
        setRawValue(docBuilder, targetPath, result.toString());
        return true;
    }

    private boolean handleSum(PipeDoc.Builder docBuilder, List<String> sourcePaths, String targetPath) {
        double sum = 0.0;
        for (String path : sourcePaths) {
            Optional<Object> value = getRawValue(docBuilder, path);
            if (value.isEmpty() || !(value.get() instanceof Number)) {
                return false; // A source field was missing or not a number
            }
            sum += ((Number) value.get()).doubleValue();
        }
        setRawValue(docBuilder, targetPath, sum);
        return true;
    }

    private boolean handleSplitMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping) {
        if (mapping.getSourceFieldPathsCount() != 1 || mapping.getTargetFieldPathsCount() < 1) {
            return false; // Invalid SPLIT mapping
        }

        String sourcePath = mapping.getSourceFieldPaths(0);
        Optional<Object> sourceValue = getRawValue(docBuilder, sourcePath);

        if (sourceValue.isEmpty() || !(sourceValue.get() instanceof String)) {
            return false; // Source field must exist and be a string
        }

        String[] splitValues = ((String) sourceValue.get()).split(mapping.getSplitConfig().getDelimiter());

        // Map the split values to the target fields, up to the number of targets specified
        for (int i = 0; i < mapping.getTargetFieldPathsCount(); i++) {
            if (i < splitValues.length) {
                String targetPath = mapping.getTargetFieldPaths(i);
                setRawValue(docBuilder, targetPath, splitValues[i]);
            }
        }

        return true;
    }

    private boolean handleTransformMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping) {
        TransformConfig config = mapping.getTransformConfig();

        // "Beefed up" transform: allow executing the ProtoFieldMapper rule syntax via params.
        // This avoids changing protos while still exposing the richer mapper features.
        String ruleName = config.getRuleName() == null ? "" : config.getRuleName().toLowerCase();
        switch (ruleName) {
            case "proto_rules":
            case "proto_rule":
            case "rule":
            case "rules":
            case "proto_field_mapper":
                return applyProtoRuleStrings(docBuilder, config);
            default:
                break;
        }

        if (mapping.getSourceFieldPathsCount() != 1 || mapping.getTargetFieldPathsCount() != 1) {
            return false; // Invalid TRANSFORM mapping
        }

        String sourcePath = mapping.getSourceFieldPaths(0);
        String targetPath = mapping.getTargetFieldPaths(0);

        Optional<Object> sourceValue = getRawValue(docBuilder, sourcePath);
        if (sourceValue.isEmpty()) {
            return false;
        }

        switch (ruleName) {
            case "uppercase":
                return transformUppercase(docBuilder, targetPath, sourceValue.get());
            case "trim":
                return transformTrim(docBuilder, targetPath, sourceValue.get());
            default:
                return false; // Unknown transformation rule
        }
    }

    /**
     * Executes {@link ProtoFieldMapperImpl} rule syntax against a {@link PipeDoc}.
     *
     * <p>Rules are passed through {@code TransformConfig.params}:
     * <ul>
     *   <li>{@code rule}: single string rule</li>
     *   <li>{@code rules}: list of string rules</li>
     * </ul>
     *
     * <p>Example rule strings:
     * <ul>
     *   <li>{@code search_metadata.custom_fields.output = search_metadata.custom_fields.headline}</li>
     *   <li>{@code search_metadata.custom_fields.flag = true}</li>
     *   <li>{@code -search_metadata.custom_fields.headline}</li>
     * </ul>
     */
    private boolean applyProtoRuleStrings(PipeDoc.Builder docBuilder, TransformConfig config) {
        if (!config.hasParams()) {
            return false;
        }

        List<String> rules = new ArrayList<>();
        var fields = config.getParams().getFieldsMap();

        Value ruleVal = fields.get("rule");
        if (ruleVal != null && ruleVal.getKindCase() == Value.KindCase.STRING_VALUE) {
            String r = ruleVal.getStringValue();
            if (r != null && !r.isBlank()) {
                rules.add(r);
            }
        }

        Value rulesVal = fields.get("rules");
        if (rulesVal != null && rulesVal.getKindCase() == Value.KindCase.LIST_VALUE) {
            for (Value v : rulesVal.getListValue().getValuesList()) {
                if (v.getKindCase() == Value.KindCase.STRING_VALUE) {
                    String r = v.getStringValue();
                    if (r != null && !r.isBlank()) {
                        rules.add(r);
                    }
                }
            }
        }

        if (rules.isEmpty()) {
            return false;
        }

        try {
            // Apply in-place so later rules can see earlier changes without rebuilding messages.
            protoFieldMapper.mapInPlace(docBuilder, rules);
            return true;
        } catch (ProtoFieldMapperImpl.MappingException e) {
            LOG.debugf(e, "Failed to apply proto rule(s): %s", rules);
            return false;
        }
    }

    private boolean transformUppercase(PipeDoc.Builder docBuilder, String targetPath, Object value) {
        if (!(value instanceof String)) {
            return false;
        }
        String upper = ((String) value).toUpperCase();
        setRawValue(docBuilder, targetPath, upper);
        return true;
    }

    private boolean transformTrim(PipeDoc.Builder docBuilder, String targetPath, Object value) {
        if (!(value instanceof String)) {
            return false;
        }
        String trimmed = ((String) value).trim();
        setRawValue(docBuilder, targetPath, trimmed);
        return true;
    }

    // ========== Field access utilities ==========

    private Optional<Object> getRawValue(PipeDoc.Builder docBuilder, String path) {
        try {
            Object raw = protoFieldMapper.getValue(docBuilder, normalizePath(path));
            if (raw == null) {
                return Optional.empty();
            }
            return Optional.of(raw);
        } catch (ProtoFieldMapperImpl.MappingException e) {
            LOG.debugf(e, "Failed to read path '%s' from PipeDoc", path);
            return Optional.empty();
        }
    }

    private void setRawValue(PipeDoc.Builder docBuilder, String path, Object value) {
        try {
            protoFieldMapper.setValue(docBuilder, normalizePath(path), value);
        } catch (ProtoFieldMapperImpl.MappingException e) {
            LOG.debugf(e, "Failed to write path '%s' on PipeDoc", path);
        }
    }

    /**
     * Maintains the current service contract: a bare path like {@code "headline"} refers to
     * {@code search_metadata.custom_fields.headline}.
     *
     * <p>If callers want to map non-custom fields, they should provide an explicit dot path.
     */
    private static String normalizePath(String path) {
        if (path == null) {
            return null;
        }
        String p = path.trim();
        if (p.isEmpty()) {
            return p;
        }
        if (p.contains(".")) {
            return p;
        }
        return "search_metadata.custom_fields." + p;
    }
}
