# Hydration Model

The Hydration Model defines how the PipeStream engine retrieves document content and metadata from persistent storage. By separating the document reference from its physical content, the engine can efficiently route large documents across the network while only fetching the binary data when strictly required by a processing module.

### Hydration Levels
- **Level 1: Document Hydration**: Resolves a `DocumentReference` into a full `PipeDoc` metadata object. This is typically done by the Kafka Sidecar or the Engine when a gRPC sender provides only a reference.
- **Level 2: Blob Hydration**: Fetches the raw binary content (bytes) from S3 via the Repo Service. This is an on-demand process triggered only if the target module (like a parser) requires the raw file for processing.
- **Dehydration**: The inverse process where a full document is replaced by a reference after persistence, ensuring Kafka message size limits (10MB) are respected.

### Engine Hydration Implementation

The engine handles hydration within its processing loop, checking the requirements of the module before execution.

```java
void processNode(PipeStream stream) {
    // 1. Level 1 Hydration: Reference -> Metadata
    PipeDoc doc;
    if (stream.hasDocumentRef()) {
        DocumentReference ref = stream.getDocumentRef();
        doc = repoService.getPipeDoc(ref.getDocId(), ref.getSourceNodeId(), ref.getAccountId());
    } else {
        doc = stream.getDocument();
    }
    
    // 2. Level 2 Hydration: Reference -> Binary Bytes
    GraphNode node = graphCache.getNode(stream.getCurrentNodeId());
    ModuleCapabilities caps = getModuleCapabilities(node.getModuleId());
    
    if (caps.needsBlobContent() && doc.getBlobBag().getBlob().hasStorageRef()) {
        FileStorageReference ref = doc.getBlobBag().getBlob().getStorageRef();
        byte[] blobData = repoService.getBlob(ref);
        
        // Inline the bytes for the module
        doc = doc.toBuilder()
            .setBlobBag(doc.getBlobBag().toBuilder()
                .setBlob(doc.getBlobBag().getBlob().toBuilder()
                    .setData(ByteString.copyFrom(blobData))
                    .clearStorageRef()
                    .build())
                .build())
            .build();
    }
    
    // Call module with hydrated document
    ProcessDataResponse response = callModule(node, doc);
}
```

### Deep Dive: Hydration Decisions

The hydration strategy is optimized for the "Fast Path" where documents move via gRPC.

- **Selective Fetching**: Level 2 hydration is only performed if `needsBlobContent()` is true. Parsers (Tika, Docling) need blobs; Chunkers and Embedders do not.
- **Sidecar Responsibility**: When documents arrive via Kafka, the Sidecar performs Level 1 hydration before the Engine ever sees the request, keeping the Engine's gRPC interface consistent.
- **Repo Service Abstraction**: Neither the Engine nor the Sidecar access S3 directly. They use the Repo Service gRPC API, which manages authentication, path resolution, and caching.
- **Storage Structure**: Documents are stored in S3 using a hierarchical path `{accountId}/{nodeId}/{docId}.pipedoc`, allowing for efficient multi-tenant isolation and cleanup.

### Visualization of Hydration Flow

```mermaid
graph TD
    subgraph KafkaPath [Kafka Sidecar Level 1]
        KS1[Receive Kafka Ref] --> KS2[Call Repo: getPipeDoc]
        KS2 --> KS3[Hydrated Metadata]
    end
    
    subgraph EnginePath [Engine Level 2]
        KS3 --> E1[Receive gRPC]
        E1 --> E2{Module needs blob?}
        E2 -- Yes --> E3[Call Repo: getBlob]
        E3 --> E4[Inlined Bytes]
        E2 -- No --> E5[Metadata Only]
    end
    
    E4 --> Module[Remote Module]
    E5 --> Module
    
    Repo[(Repo Service)]
    S3[(S3 Storage)]
    
    KS2 -.-> Repo
    E3 -.-> Repo
    Repo --- S3
```

### Dehydration Flow (Post-Processing)

After a module completes its work, the engine may dehydrate the document before routing it to the next node, especially if the next hop is via Kafka.

```java
void routeToNextNode(PipeStream stream, PipeDoc doc, GraphEdge edge) {
    if (edge.getTransportType() == TRANSPORT_TYPE_MESSAGING) {
        // MUST persist and dehydrate for Kafka
        repoService.savePipeDoc(doc, stream.getCurrentNodeId(), stream.getAccountId());
        
        PipeStream dehydrated = stream.toBuilder()
            .clearDocument()
            .setDocumentRef(DocumentReference.newBuilder()
                .setDocId(doc.getDocId())
                .setSourceNodeId(stream.getCurrentNodeId())
                .setAccountId(stream.getAccountId())
                .build())
            .build();
            
        kafkaProducer.send(edge.getKafkaTopic(), dehydrated);
    }
}
```

```mermaid
sequenceDiagram
    participant E as Engine
    participant R as Repo Service
    participant K as Kafka
    
    E->>R: savePipeDoc(full_doc)
    R-->>E: Ack
    E->>E: Clear doc field, Set doc_ref
    E->>K: publish(dehydrated_stream)
```
