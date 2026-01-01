-- Create datasource_instances table for storing DatasourceInstance bindings
-- DatasourceInstances bind datasources (from datasource-admin) to entry nodes in a graph

CREATE TABLE datasource_instances (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    datasource_instance_id  VARCHAR(512) NOT NULL UNIQUE,  -- Format: {graph_id}:{version}:{datasource_id}
    graph_id                VARCHAR(255) NOT NULL,          -- Logical graph identifier
    version                 BIGINT NOT NULL,                -- Graph version this instance belongs to
    datasource_id           VARCHAR(255) NOT NULL,          -- References datasource from datasource-admin
    entry_node_id           VARCHAR(255) NOT NULL,          -- Entry node in the graph
    node_config_json        JSONB,                          -- Tier 2 NodeConfig as JSON (optional)
    created_at              TIMESTAMP DEFAULT NOW(),
    created_by              VARCHAR(255),

    -- Each datasource can only have one instance per graph version
    UNIQUE(graph_id, version, datasource_id)
);

-- Index for looking up instances by graph (used when loading active graph)
CREATE INDEX idx_datasource_instances_graph ON datasource_instances(graph_id, version);

-- Index for looking up all instances for a datasource (impact analysis)
CREATE INDEX idx_datasource_instances_datasource ON datasource_instances(datasource_id);

-- Index for entry node lookups
CREATE INDEX idx_datasource_instances_entry_node ON datasource_instances(entry_node_id);

