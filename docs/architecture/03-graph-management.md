# Graph Management

## Storage Model

Graphs are stored as complete JSON snapshots in Postgres:

```sql
CREATE TABLE pipeline_graphs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id        VARCHAR(255) NOT NULL,      -- Logical graph identifier
    cluster_id      VARCHAR(255) NOT NULL,
    account_id      VARCHAR(255) NOT NULL,
    version         BIGINT NOT NULL,            -- Auto-increment per graph_id
    graph_data      JSONB NOT NULL,             -- Full PipelineGraph proto as JSON
    is_active       BOOLEAN DEFAULT false,
    created_at      TIMESTAMP DEFAULT NOW(),
    created_by      VARCHAR(255),
    
    UNIQUE(graph_id, version)
);

CREATE INDEX idx_graphs_active ON pipeline_graphs(graph_id, is_active) WHERE is_active = true;
CREATE INDEX idx_graphs_cluster ON pipeline_graphs(cluster_id);
```

### Why Full Snapshots?

- **Simple**: No complex diff/merge logic
- **Auditable**: Every version is complete and self-contained
- **Fast Load**: Single read to load entire graph
- **Rollback**: Activate any previous version instantly

## Graph Update Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Frontend   │────▶│   Config    │────▶│  Postgres   │────▶│    Kafka    │
│  (Designer) │     │   Service   │     │             │     │ graph-updates│
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                   │
                         ┌─────────────────────────────────────────┘
                         ▼
                    ┌─────────────┐
                    │   Engine    │
                    │  (rebuild   │
                    │   cache)    │
                    └─────────────┘
```

1. **Frontend** saves graph via Config Service
2. **Config Service** writes new version to Postgres (within transaction)
3. **Config Service** publishes full graph snapshot to Kafka `graph-updates` topic
4. **All Engines** consume update and rebuild in-memory cache

## Engine In-Memory Cache

```java
public class GraphCache {
    // Full graph proto
    private volatile PipelineGraph graph;
    private volatile long version;
    
    // Helper indexes for fast lookups
    private Map<String, GraphNode> nodeById;
    private Map<String, List<GraphEdge>> outgoingEdgesByNode;
    private Map<String, CelProgram> compiledConditions;
    private Map<String, String> nodeToKafkaTopic;
    
    public void rebuild(PipelineGraph newGraph) {
        // Build indexes
        Map<String, GraphNode> newNodeById = new HashMap<>();
        for (String nodeId : newGraph.getNodeIdsList()) {
            newNodeById.put(nodeId, lookupNode(nodeId));
        }
        
        Map<String, List<GraphEdge>> newEdgesByNode = new HashMap<>();
        for (GraphEdge edge : newGraph.getEdgesList()) {
            newEdgesByNode
                .computeIfAbsent(edge.getFromNodeId(), k -> new ArrayList<>())
                .add(edge);
        }
        
        // Pre-compile CEL expressions
        Map<String, CelProgram> newConditions = new HashMap<>();
        for (GraphEdge edge : newGraph.getEdgesList()) {
            if (edge.hasCondition()) {
                newConditions.put(edge.getEdgeId(), 
                    celCompiler.compile(edge.getCondition()));
            }
        }
        
        // Atomic swap
        this.graph = newGraph;
        this.version = newGraph.getVersion();
        this.nodeById = newNodeById;
        this.outgoingEdgesByNode = newEdgesByNode;
        this.compiledConditions = newConditions;
    }
    
    public GraphNode getNode(String nodeId) {
        return nodeById.get(nodeId);
    }
    
    public List<GraphEdge> getOutgoingEdges(String nodeId) {
        return outgoingEdgesByNode.getOrDefault(nodeId, List.of());
    }
    
    public CelProgram getCompiledCondition(String edgeId) {
        return compiledConditions.get(edgeId);
    }
}
```

## Versioning Strategy

```java
@Transactional
public PipelineGraph saveGraph(PipelineGraph graph) {
    // Get next version
    long nextVersion = graphRepo.getMaxVersion(graph.getGraphId()) + 1;
    
    // Deactivate current active
    graphRepo.deactivateAll(graph.getGraphId());
    
    // Insert new version as active
    PipelineGraph versioned = graph.toBuilder()
        .setVersion(nextVersion)
        .build();
    
    graphRepo.insert(GraphEntity.builder()
        .graphId(graph.getGraphId())
        .clusterId(graph.getClusterId())
        .version(nextVersion)
        .graphData(toJson(versioned))
        .isActive(true)
        .build());
    
    // Publish to Kafka
    kafkaProducer.send("graph-updates", versioned);
    
    return versioned;
}
```

## In-Flight Stream Handling

When graph updates mid-processing:

- **Streams use latest graph at each hop** - no version pinning
- **Orphaned nodes** - stream routes to node that no longer exists → DLQ
- **New nodes** - immediately available for routing
- **Reprocessing** - replay from Kafka with new graph version

This is intentional: graphs should be backward compatible, and breaking changes require reprocessing anyway.

## Multi-Tenant Isolation

```sql
-- Graphs are scoped by account
SELECT * FROM pipeline_graphs 
WHERE account_id = ? AND cluster_id = ? AND is_active = true;
```

Engines load graphs for their assigned cluster(s). Cross-cluster routing uses `CrossClusterEdge` with explicit `to_cluster_id`.
