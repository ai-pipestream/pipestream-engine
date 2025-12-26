package ai.pipestream.engine.hydration;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocRequest;
import ai.pipestream.repository.pipedoc.v1.MutinyPipeDocServiceGrpc;
import ai.pipestream.repository.pipedoc.v1.SavePipeDocRequest;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Client for interacting with the Repository Service.
 * Handles hydration (fetching documents) and dehydration (saving documents).
 * 
 * Uses DynamicGrpcClientFactory for reactive, non-blocking gRPC calls.
 */
@ApplicationScoped
public class RepoClient {

    private static final Logger LOG = Logger.getLogger(RepoClient.class);

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    private static final String REPOSITORY_SERVICE_NAME = "repository-service";

    /**
     * Fetches a fully hydrated PipeDoc from the repository.
     * 
     * @param nodeId The repository node ID (which tracks the doc).
     * @return A Uni containing the PipeDoc.
     */
    public Uni<PipeDoc> getPipeDoc(String nodeId) {
        LOG.debugf("Fetching PipeDoc for node ID: %s", nodeId);
        
        GetPipeDocRequest request = GetPipeDocRequest.newBuilder()
                .setNodeId(nodeId)
                .build();

        return grpcClientFactory.getClient(REPOSITORY_SERVICE_NAME, MutinyPipeDocServiceGrpc::newMutinyStub)
                .flatMap(stub -> stub.getPipeDoc(request))
                .map(response -> response.getPipedoc());
    }

    /**
     * Saves a PipeDoc to the repository (Dehydration).
     * This is required before sending a reference over Kafka.
     * 
     * @param doc The document to save.
     * @param drive The drive/bucket to store it in.
     * @return A Uni containing the saved document's node ID.
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
                .map(response -> response.getNodeId());
    }
}
