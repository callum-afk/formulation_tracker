-- Add batch archive columns so admins can hide old batches from default formulation selection flows.
ALTER TABLE `PROJECT_ID.DATASET_ID.ingredient_batches`
ADD COLUMN IF NOT EXISTS archived BOOL;

-- Add archive timestamp for minimal auditability when a batch is archived.
ALTER TABLE `PROJECT_ID.DATASET_ID.ingredient_batches`
ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP;

-- Add archive actor email for minimal auditability when a batch is archived.
ALTER TABLE `PROJECT_ID.DATASET_ID.ingredient_batches`
ADD COLUMN IF NOT EXISTS archived_by STRING;

-- Backfill null archive flags from pre-migration rows to explicit FALSE for deterministic filters.
UPDATE `PROJECT_ID.DATASET_ID.ingredient_batches`
SET archived = FALSE
WHERE archived IS NULL;
