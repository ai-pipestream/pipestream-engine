# Hydration Model

## Overview

Hydration is the process of fetching document data from storage when only a reference is available. PipeStream has **two independent levels** of hydration.

## Two-Level Hydration

```
┌─────────────────────────────────────────────────────────────────────┐
│                    HYDRATION LEVELS                                  │
│                                                                      │
│  Level 1: PipeStream → PipeDoc                                      │
│  ─────────────────────────────────                                  │
│  Question: Do I have the document?                                  │
│                                                                      │
│  PipeStream contains:                                               │
│  ├── OPTION A: PipeDoc document (inline)                            │
│  └── OPTION B: DocumentReference document_ref (stored in repo)      │
│                                                                      │
│  If document_ref → call Repo.GetPipeDoc() to hydrate                │
│                                                                      │
│  ───────────────────────────────────────────────────────────────    │
│                                                                      │
│  Level 2: PipeDoc → Blob Content                                    │
│  ────────────────────────────────                                   │
│  Question: Do I need the binary content?                            │
│                                                                      │
│  PipeDoc.blob_bag.blob contains:                                    │
│  ├── OPTION A: bytes data (inline)                                  │
│  ├── OPTION B: FileStorageReference storage_ref (in S3)             │
│  └── OPTION C: Empty/dropped (binary no longer needed)              │
│                                                                      │
│  If storage_ref AND module needs blob → hydrate from Repo           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## DocumentReference (Level 1)

```protobuf
// Reference to a PipeDoc stored in the repository.
message DocumentReference {
  // Document identifier.
  string doc_id = 1;
  // Node that produced this version (determines storage location).
  string source_node_id = 2;
  // Account context for multi-tenant lookup.
  string account_id = 3;
}
```

**S3 Storage Structure:**
```
s3://bucket/{node-uuid}/
  ├── {doc-id-1}.pipedoc
  ├── {doc-id-2}.pipedoc
  └── ... millions of docs ...
```

Each node instance has its own directory. Millions of documents flow through each node.

## Transport Constraints

| Transport | Document Storage | Reason |
|-----------|------------------|--------|
| **Kafka** | Always `document_ref` | 10MB message limit |
| **gRPC** | Either inline or `document_ref` | 2GB limit allows flexibility |

## Blob Hydration Decision (Level 2)

| Module Type | Needs Blob? | Action |
|-------------|-------------|--------|
| **Parser** (Tika, Docling) | Yes | Hydrate `storage_ref` → `bytes data` |
| **Chunker** | No | Skip hydration |
| **Embedder** | No | Skip hydration |
| **Enricher** | Usually no | Skip unless config says otherwise |
| **Sink** | Depends | Config-driven |

## Blob Lifecycle After Parsing

```
Document enters pipeline:
  └── blob_bag.blob.data = [original Word doc bytes]

After parser (Tika) processes:
  └── parsed_metadata populated
  └── blob_bag options:
      ├── KEEP INLINE: blob.data still present (for parser→parser)
      ├── DEHYDRATE: blob.storage_ref points to S3, data cleared
      └── DROP: blob_bag empty (binary no longer needed)

Rest of pipeline (chunker, embedder, sink):
  └── Works with parsed_metadata, search_metadata
  └── No need for original binary
```

## Engine Hydration Logic

```java
void processNode(PipeStream stream) {
    // Level 1: Hydrate PipeStream → PipeDoc
    PipeDoc doc;
    if (stream.hasDocumentRef()) {
        DocumentReference ref = stream.getDocumentRef();
        doc = repoService.getPipeDoc(ref.getDocId(), ref.getSourceNodeId(), ref.getAccountId());
    } else {
        doc = stream.getDocument();
    }
    
    // Level 2: Hydrate Blob if module needs it
    GraphNode node = graphCache.getNode(stream.getCurrentNodeId());
    ModuleCapabilities caps = getModuleCapabilities(node.getModuleId());
    
    if (caps.needsBlobContent() && doc.getBlobBag().getBlob().hasStorageRef()) {
        FileStorageReference ref = doc.getBlobBag().getBlob().getStorageRef();
        byte[] blobData = repoService.getBlob(ref);
        doc = doc.toBuilder()
            .setBlobBag(doc.getBlobBag().toBuilder()
                .setBlob(doc.getBlobBag().getBlob().toBuilder()
                    .setData(ByteString.copyFrom(blobData))
                    .clearStorageRef()
                    .build())
                .build())
            .build();
    }
    
    // Now call module with appropriately hydrated doc
    ProcessDataResponse response = callModule(node, doc);
    // ...
}
```

## Dehydration (After Module Processing)

```java
void routeToNextNode(PipeStream stream, PipeDoc doc, GraphEdge edge) {
    if (edge.getTransportType() == TRANSPORT_TYPE_MESSAGING) {
        // Kafka edge: MUST persist, use document_ref
        String nodeId = stream.getCurrentNodeId();
        repoService.savePipeDoc(doc, nodeId, stream.getAccountId());
        
        PipeStream dehydrated = stream.toBuilder()
            .clearDocument()
            .setDocumentRef(DocumentReference.newBuilder()
                .setDocId(doc.getDocId())
                .setSourceNodeId(nodeId)
                .setAccountId(stream.getAccountId())
                .build())
            .build();
        
        kafkaProducer.send(edge.getKafkaTopic(), dehydrated);
        
    } else {
        // gRPC edge: can pass inline (or dehydrate for large docs)
        if (shouldDehydrate(doc)) {
            // Same as above
        } else {
            // Pass inline
            engineClient.processNode(stream.toBuilder()
                .setDocument(doc)
                .build());
        }
    }
}
```

## No-Persistence Fast Path

A document can flow through the entire pipeline without ever touching S3:

```
Intake ──gRPC──► Engine ──gRPC──► Parser ──gRPC──► Engine ──gRPC──► Chunker ...
                   │                                  │
                   └── inline doc ────────────────────┘
                       (never persisted until sink)
```

This is the fast path for:
- Small documents
- All gRPC edges
- Latency-sensitive processing

## Multi-Parser Scenario

```
Tika (parser) ──► Docling (parser)

Between parsers:
  └── blob.data kept INLINE (Docling needs the binary)

After Docling:
  └── blob dehydrated or dropped (no more parsers need it)
```

The engine tracks whether downstream nodes need the blob and makes the hydration/dehydration decision accordingly.
