package ai.pipestream.engine.hydration;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.pipedoc.v1.*;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Client for interacting with the Repository Service.
 * Handles hydration (fetching documents) and dehydration (saving documents).
 * <p>
 * Uses DynamicGrpcClientFactory for reactive, non-blocking gRPC calls.
 */
@ApplicationScoped
public class RepoClient {

    /** Logger for this service class. */
    private static final Logger LOG = Logger.getLogger(RepoClient.class);

    /** Injected factory for creating dynamic gRPC clients. */
    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    /** The service name for the repository service as registered in service discovery. */
    private static final String REPOSITORY_SERVICE_NAME = "repository-service";

    /**
     * Fetches a fully hydrated PipeDoc from the repository.
     * <p>
     * Uses dynamic gRPC client creation to connect to the repository service
     * and retrieve the complete document data by its node identifier.
     *
     * @param nodeId The repository node ID that tracks the document
     * @return A Uni that completes with the fully hydrated PipeDoc
     */
    public Uni<PipeDoc> getPipeDoc(String nodeId) {
        LOG.debugf("Fetching PipeDoc for node ID: %s", nodeId);
        
        GetPipeDocRequest request = GetPipeDocRequest.newBuilder()
                .setNodeId(nodeId)
                .build();

        return grpcClientFactory.getClient(REPOSITORY_SERVICE_NAME, MutinyPipeDocServiceGrpc::newMutinyStub)
                .flatMap(stub -> stub.getPipeDoc(request))
                .map(GetPipeDocResponse::getPipedoc);
    }

    /**
     * Saves a PipeDoc to the repository (Dehydration).
     * <p>
     * This is required before sending a reference over Kafka to avoid
     * duplicating large document payloads. The document is stored in the
     * repository and a lightweight reference is sent instead.
     *
     * @param doc The document to save to the repository
     * @param drive The drive/bucket identifier where to store the document
     * @return A Uni that completes with the saved document's node ID for referencing
     */
    public Uni<String> savePipeDoc(PipeDoc doc, String drive) {
        LOG.debugf("Saving PipeDoc with ID: %s to drive: %s", doc.getDocId(), drive);
        
        SavePipeDocRequest.Builder requestBuilder = SavePipeDocRequest.newBuilder()
                .setPipedoc(doc)
                .setDrive(drive);
        
        if (doc.hasOwnership() && doc.getOwnership().hasConnectorId()) {
            requestBuilder.setConnectorId(doc.getOwnership().getConnectorId());
        }

        SavePipeDocRequest request = requestBuilder.build();

        return grpcClientFactory.getClient(REPOSITORY_SERVICE_NAME, MutinyPipeDocServiceGrpc::newMutinyStub)
                .flatMap(stub -> stub.savePipeDoc(request))
                .map(SavePipeDocResponse::getNodeId);
    }
}
