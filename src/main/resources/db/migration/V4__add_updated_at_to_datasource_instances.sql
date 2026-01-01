-- Add updated_at column to datasource_instances for audit tracking
-- This field tracks when the node_config was last modified

ALTER TABLE datasource_instances
    ADD COLUMN updated_at TIMESTAMP;

-- Set initial value to created_at for existing rows
UPDATE datasource_instances SET updated_at = created_at WHERE updated_at IS NULL;
