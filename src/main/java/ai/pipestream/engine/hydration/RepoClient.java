package ai.pipestream.engine.hydration;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.FileStorageReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.pipedoc.v1.*;
import com.google.protobuf.ByteString;
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
     * Fetches a fully hydrated PipeDoc from the repository by repository node ID.
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
     * Fetches a fully hydrated PipeDoc from the repository using a DocumentReference.
     * <p>
     * This is the preferred method for Engine hydration when documents are referenced
     * via doc_id, source_node_id, and account_id. Uses dynamic gRPC client creation
     * to connect to the repository service and retrieve the complete document data
     * based on logical identifiers.
     *
     * @param documentRef The document reference containing doc_id, source_node_id, and account_id
     * @return A Uni that completes with the fully hydrated PipeDoc
     */
    public Uni<PipeDoc> getPipeDocByReference(DocumentReference documentRef) {
        LOG.debugf("Fetching PipeDoc by reference: doc_id=%s, source_node_id=%s, account_id=%s", 
                documentRef.getDocId(), documentRef.getSourceNodeId(), documentRef.getAccountId());
        
        GetPipeDocByReferenceRequest request = GetPipeDocByReferenceRequest.newBuilder()
                .setDocumentRef(documentRef)
                .build();

        return grpcClientFactory.getClient(REPOSITORY_SERVICE_NAME, MutinyPipeDocServiceGrpc::newMutinyStub)
                .flatMap(stub -> stub.getPipeDocByReference(request))
                .map(GetPipeDocByReferenceResponse::getPipedoc);
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
     * @param graphLocationId The graph location ID (node ID) where this document was processed.
     *                        If null, the repository service will use datasource_id as fallback.
     * @param clusterId The cluster ID where this document was processed (null for intake)
     * @return A Uni that completes with the saved document's node ID (UUID) for referencing
     */
    public Uni<String> savePipeDoc(PipeDoc doc, String drive, String graphLocationId, String clusterId) {
        LOG.debugf("Saving PipeDoc with ID: %s to drive: %s, graph_location_id: %s, cluster_id: %s", 
                doc.getDocId(), drive, graphLocationId, clusterId != null ? clusterId : "null (intake)");
        
        SavePipeDocRequest.Builder requestBuilder = SavePipeDocRequest.newBuilder()
                .setPipedoc(doc)
                .setDrive(drive);
        
        if (doc.hasOwnership() && doc.getOwnership().hasConnectorId()) {
            requestBuilder.setConnectorId(doc.getOwnership().getConnectorId());
        }

        // Set graph_address oneof: either graph_location_id or use_datasource_id
        if (graphLocationId != null && !graphLocationId.isEmpty()) {
            requestBuilder.setGraphLocationId(graphLocationId);
        } else {
            // If no graph_location_id provided, use datasource_id (for initial intake)
            requestBuilder.setUseDatasourceId(true);
        }
        
        // Set cluster_id if provided (null for intake, cluster name for cluster processing)
        if (clusterId != null && !clusterId.isEmpty()) {
            requestBuilder.setClusterId(clusterId);
        }

        SavePipeDocRequest request = requestBuilder.build();

        return grpcClientFactory.getClient(REPOSITORY_SERVICE_NAME, MutinyPipeDocServiceGrpc::newMutinyStub)
                .flatMap(stub -> stub.savePipeDoc(request))
                .map(SavePipeDocResponse::getNodeId);
    }

    /**
     * Fetches blob binary content from the repository using a FileStorageReference (Level 2 Hydration).
     * <p>
     * This is used when a module requires the raw binary content (e.g., parsers like Tika).
     * The FileStorageReference is obtained from a PipeDoc's BlobBag after Level 1 hydration.
     *
     * @param storageRef The file storage reference containing drive name, object key, and optional version ID
     * @return A Uni that completes with the blob binary data as a ByteString
     */
    public Uni<com.google.protobuf.ByteString> getBlob(ai.pipestream.data.v1.FileStorageReference storageRef) {
        LOG.debugf("Fetching blob from repository: drive=%s, object_key=%s", 
                storageRef.getDriveName(), storageRef.getObjectKey());
        
        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(storageRef)
                .build();

        return grpcClientFactory.getClient(REPOSITORY_SERVICE_NAME, MutinyPipeDocServiceGrpc::newMutinyStub)
                .flatMap(stub -> stub.getBlob(request))
                .map(GetBlobResponse::getData);
    }
}
