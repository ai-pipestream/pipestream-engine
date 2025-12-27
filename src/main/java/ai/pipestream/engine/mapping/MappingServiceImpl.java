package ai.pipestream.engine.mapping;

import ai.pipestream.data.v1.AggregateConfig;
import ai.pipestream.data.v1.CelConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.ProcessingMapping;
import ai.pipestream.data.v1.TransformConfig;
import ai.pipestream.engine.mapping.util.ProtoFieldMapperImpl;
import ai.pipestream.engine.routing.CelEvaluatorService;
import ai.pipestream.mapping.v1.ApplyMappingRequest;
import ai.pipestream.mapping.v1.ApplyMappingResponse;
import ai.pipestream.mapping.v1.MappingRule;
import ai.pipestream.mapping.v1.MutinyMappingServiceGrpc;
import com.google.protobuf.Value;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@GrpcService
public class MappingServiceImpl extends MutinyMappingServiceGrpc.MappingServiceImplBase {

    private static final Logger LOG = Logger.getLogger(MappingServiceImpl.class);

    private final ProtoFieldMapperImpl protoFieldMapper;
    private final CelEvaluatorService celEvaluatorService;

    /**
     * Default constructor for non-CDI usage (e.g., plain unit tests).
     */
    public MappingServiceImpl() {
        this.protoFieldMapper = new ProtoFieldMapperImpl();
        this.celEvaluatorService = new CelEvaluatorService();
        this.celEvaluatorService.init();
    }

    /**
     * CDI constructor.
     */
    @Inject
    public MappingServiceImpl(ProtoFieldMapperImpl protoFieldMapper, CelEvaluatorService celEvaluatorService) {
        this.protoFieldMapper = protoFieldMapper;
        this.celEvaluatorService = celEvaluatorService;
    }

    @Override
    public Uni<ApplyMappingResponse> applyMapping(ApplyMappingRequest request) {
        PipeDoc.Builder docBuilder = request.getDocument().toBuilder();

        for (MappingRule rule : request.getRulesList()) {
            applyRule(docBuilder, rule);
        }

        return Uni.createFrom().item(ApplyMappingResponse.newBuilder()
                .setDocument(docBuilder.build())
                .build());
    }

    private void applyRule(PipeDoc.Builder docBuilder, MappingRule rule) {
        for (ProcessingMapping candidate : rule.getCandidateMappingsList()) {
            boolean success = false;
            switch (candidate.getMappingType()) {
                case MAPPING_TYPE_DIRECT:
                    success = handleDirectMapping(docBuilder, candidate);
                    break;
                case MAPPING_TYPE_TRANSFORM:
                    success = handleTransformMapping(docBuilder, candidate);
                    break;
                case MAPPING_TYPE_AGGREGATE:
                    success = handleAggregateMapping(docBuilder, candidate);
                    break;
                case MAPPING_TYPE_SPLIT:
                    success = handleSplitMapping(docBuilder, candidate);
                    break;
                case MAPPING_TYPE_CEL:
                    success = handleCelMapping(docBuilder, candidate);
                    break;
                case MAPPING_TYPE_UNSPECIFIED:
                default:
                    // Do nothing for unspecified or unknown types
                    break;
            }
            if (success) {
                return; // Move to the next rule once a candidate succeeds
            }
        }
    }

    private boolean handleCelMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping) {
        if (!mapping.hasCelConfig() || mapping.getTargetFieldPathsCount() == 0) {
            return false;
        }

        CelConfig config = mapping.getCelConfig();
        String targetPath = mapping.getTargetFieldPaths(0);

        // Wrap the current document builder state into a PipeStream for CEL evaluation.
        // This allows the CEL expression to access 'document'.
        PipeStream context = PipeStream.newBuilder()
                .setDocument(docBuilder.build())
                .build();

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