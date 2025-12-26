# Graph Management

Graph management ensures that pipeline definitions are stored durably, updated reliably across a distributed cluster, and cached efficiently for high-performance execution. It handles versioning, multi-tenancy, and the atomic swap of graph definitions in the engine's memory.

## Storage Model

Pipestream uses a snapshot-based storage model for pipeline graphs. Instead of storing complex diffs, every change results in a complete, new version of the graph stored as a JSON blob.

### Database Schema
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

### Key Design Choices
- **Full Snapshots**: Every version is complete and self-contained, eliminating complex reconstruction logic.
- **Auditability**: A perfect history of every pipeline state is preserved.
- **Fast Activation**: Single database read to load the entire graph for any version.
- **Atomic Rollbacks**: Any previous version can be set to `is_active = true` to revert changes instantly.

## Graph Update Flow

The update flow coordinates the transition from a user's design change to active execution across all engine instances. It uses a combination of synchronous database writes and asynchronous event distribution.

### Update Workflow
- **State Persistence**: The Config Service writes the new graph snapshot to Postgres within a transaction.
- **Event Broadcast**: A full snapshot is published to the `graph-updates` Kafka topic.
- **Distributed Cache Invalidation**: All Engine instances consuming the topic receive the update and trigger an internal rebuild.

```mermaid
flowchart LR
    Frontend[Frontend Designer] --> Config[Config Service]
    Config --> DB[(Postgres)]
    Config --> Kafka[[Kafka: graph-updates]]
    Kafka --> Engine1[Engine Instance 1]
    Kafka --> Engine2[Engine Instance 2]
    Kafka --> EngineN[Engine Instance N]
    
    subgraph Storage [Reliable Storage]
        DB
    end
    
    subgraph Distribution [Event Bus]
        Kafka
    end
```

## Engine In-Memory Cache

To minimize latency during document processing, Engines maintain a highly optimized, in-memory representation of the active graph. This cache includes pre-compiled expressions and specialized lookups.

### Cache Implementation
```java
public class GraphCache {
    // Full graph proto
    private volatile PipelineGraph graph;
    private volatile long version;
    
    // Helper indexes for fast lookups
    private Map<String, GraphNode> nodeById;
    private Map<String, List<GraphEdge>> outgoingEdgesByNode;
    private Map<String, CelProgram> compiledConditions;
    
    public void rebuild(PipelineGraph newGraph) {
        // 1. Build Node Index
        Map<String, GraphNode> newNodeById = new HashMap<>();
        for (String nodeId : newGraph.getNodeIdsList()) {
            newNodeById.put(nodeId, lookupNode(nodeId));
        }
        
        // 2. Build Adjacency List (Edges)
        Map<String, List<GraphEdge>> newEdgesByNode = new HashMap<>();
        for (GraphEdge edge : newGraph.getEdgesList()) {
            newEdgesByNode
                .computeIfAbsent(edge.getFromNodeId(), k -> new ArrayList<>())
                .add(edge);
        }
        
        // 3. Pre-compile CEL expressions
        Map<String, CelProgram> newConditions = new HashMap<>();
        for (GraphEdge edge : newGraph.getEdgesList()) {
            if (edge.hasCondition()) {
                newConditions.put(edge.getEdgeId(), 
                    celCompiler.compile(edge.getCondition()));
            }
        }
        
        // 4. Atomic swap
        this.graph = newGraph;
        this.version = newGraph.getVersion();
        this.nodeById = newNodeById;
        this.outgoingEdgesByNode = newEdgesByNode;
        this.compiledConditions = newConditions;
    }
}
```

### Optimization Details
- **Node Indexing**: O(1) lookup of node configurations by ID.
- **Adjacency Mapping**: Pre-calculated outgoing edges to speed up routing decisions.
- **CEL Pre-compilation**: Compiling Common Expression Language (CEL) conditions once during rebuild, rather than on every document hop.
- **Atomic Swapping**: Uses `volatile` references to ensure that processing threads always see a consistent version of the graph during a switch.

```mermaid
graph TD
    Update[Kafka Update Received] --> Parse[Parse PipelineGraph Proto]
    Parse --> BuildNodes[Index Nodes by ID]
    Parse --> BuildEdges[Group Edges by Source Node]
    Parse --> CompileCEL[Pre-compile Edge Conditions]
    
    BuildNodes --> Swap[Atomic Reference Swap]
    BuildEdges --> Swap
    CompileCEL --> Swap
    
    Swap --> Active[Active Cache Used by Processing Loop]
```

## Versioning Strategy

The versioning strategy ensures that updates are sequential and that only one version of a graph is "active" for a cluster at any given time.

### Implementation Logic
- **Version Increment**: Calculates the next version number based on the current maximum for that graph ID.
- **Exclusive Activation**: Deactivates all existing versions before inserting the new one as the active state.
- **Atomic Transaction**: Database write and Kafka publication are coordinated to ensure consistency.

```java
@Transactional
public PipelineGraph saveGraph(PipelineGraph graph) {
    // 1. Get next version
    long nextVersion = graphRepo.getMaxVersion(graph.getGraphId()) + 1;
    
    // 2. Deactivate current active versions
    graphRepo.deactivateAll(graph.getGraphId());
    
    // 3. Prepare versioned snapshot
    PipelineGraph versioned = graph.toBuilder()
        .setVersion(nextVersion)
        .build();
    
    // 4. Insert new active version
    graphRepo.insert(GraphEntity.builder()
        .graphId(graph.getGraphId())
        .clusterId(graph.getClusterId())
        .version(nextVersion)
        .graphData(toJson(versioned))
        .isActive(true)
        .build());
    
    // 5. Broadcast update
    kafkaProducer.send("graph-updates", versioned);
    
    return versioned;
}
```

## In-Flight Stream Handling

Pipestream prioritizes eventual consistency and simplicity when a graph is updated while documents are still being processed.

### Handling Behaviors
- **Latest Graph Wins**: Documents always use the most recent version of the graph available at the current engine instance. There is no "version pinning" for a stream's entire journey.
- **Orphaned Nodes**: If a node is deleted while a document is in flight to it, the routing fails and the document is sent to the Dead Letter Queue (DLQ).
- **Backward Compatibility**: It is recommended that graph changes remain backward compatible to avoid processing interruptions.

```mermaid
sequenceDiagram
    participant S as Stream (In-flight)
    participant E as Engine
    participant C as Cache
    participant K as Kafka (Updates)

    Note over S,E: Processing Node A (v1)
    K->>C: Update Graph to v2
    C->>C: Rebuild Cache (v2)
    S->>E: Route to Node B
    E->>C: Lookup Node B in Cache
    C-->>E: Return Node B (v2)
    Note over S,E: Processing Node B (v2)
```

## Multi-Tenant Isolation

Isolation is maintained at the database and engine levels to ensure that processing logic and data do not leak between accounts or clusters.

### Isolation Mechanisms
- **Account Scoping**: Every graph is explicitly tied to an `account_id`.
- **Cluster Assignment**: Engines only load graphs assigned to their specific `cluster_id`.
- **Cross-Cluster Routing**: Explicitly handled via `CrossClusterEdge` types which define target clusters.

```sql
-- Engines load only their assigned active graphs
SELECT * FROM pipeline_graphs 
WHERE account_id = ? 
  AND cluster_id = ? 
  AND is_active = true;
```
