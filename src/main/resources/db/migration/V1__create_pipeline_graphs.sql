-- Create pipeline_graphs table for storing pipeline graph definitions
-- Uses snapshot-based storage model: each version is a complete, self-contained graph stored as JSONB

CREATE TABLE pipeline_graphs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id        VARCHAR(255) NOT NULL,      -- Logical graph identifier
    cluster_id      VARCHAR(255) NOT NULL,      -- Cluster this graph belongs to
    account_id      VARCHAR(255) NOT NULL,      -- Account identifier for multi-tenant isolation
    version         BIGINT NOT NULL,            -- Auto-increment per graph_id
    graph_data      JSONB NOT NULL,             -- Full PipelineGraph proto as JSON
    is_active       BOOLEAN DEFAULT false,      -- Whether this version is currently active
    created_at      TIMESTAMP DEFAULT NOW(),    -- Timestamp when this graph version was created
    created_by      VARCHAR(255),               -- User or service that created this graph version
    
    UNIQUE(graph_id, version)
);

-- Index for fast lookup of active graphs (used by engines on startup)
CREATE INDEX idx_graphs_active ON pipeline_graphs(graph_id, is_active) WHERE is_active = true;

-- Index for cluster-based queries (used for multi-cluster deployments)
CREATE INDEX idx_graphs_cluster ON pipeline_graphs(cluster_id);

