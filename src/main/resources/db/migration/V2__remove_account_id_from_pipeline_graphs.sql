-- Remove account_id column from pipeline_graphs table
-- Graphs belong to the platform/app, not to accounts.
-- Accounts are data owners (tied to datasources), not graph owners.

ALTER TABLE pipeline_graphs DROP COLUMN IF EXISTS account_id;

