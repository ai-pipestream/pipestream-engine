package ai.pipestream.engine.mapping;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.ProcessingMapping;
import ai.pipestream.mapping.v1.ApplyMappingRequest;
import ai.pipestream.mapping.v1.ApplyMappingResponse;
import ai.pipestream.mapping.v1.MappingRule;
import ai.pipestream.mapping.v1.MutinyMappingServiceGrpc;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

/**
 * gRPC service implementation for the MappingService.
 * <p>
 * Implements the MappingRule candidate fallback semantics: for each rule,
 * try candidates in order until one succeeds, then move to the next rule.
 * <p>
 * Delegates actual mapping logic to {@link MappingEngine}.
 */
@GrpcService
public class MappingServiceImpl extends MutinyMappingServiceGrpc.MappingServiceImplBase {

    private final MappingEngine mappingEngine;

    /**
     * Default constructor for non-CDI usage (e.g., plain unit tests).
     */
    public MappingServiceImpl() {
        this.mappingEngine = new MappingEngine();
    }

    /**
     * CDI constructor.
     */
    @Inject
    public MappingServiceImpl(MappingEngine mappingEngine) {
        this.mappingEngine = mappingEngine;
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

    /**
     * Applies a MappingRule with candidate fallback semantics.
     * <p>
     * Tries each candidate mapping in order. Once one succeeds, moves to the next rule.
     * This allows fallback behavior where you can specify multiple ways to map a field
     * and use the first one that works.
     */
    private void applyRule(PipeDoc.Builder docBuilder, MappingRule rule) {
        for (ProcessingMapping candidate : rule.getCandidateMappingsList()) {
            // Build stream context for CEL evaluation
            PipeStream context = PipeStream.newBuilder()
                    .setDocument(docBuilder.build())
                    .build();

            boolean success = mappingEngine.applyMapping(docBuilder, candidate, context);
            if (success) {
                return; // Move to the next rule once a candidate succeeds
            }
        }
    }
}