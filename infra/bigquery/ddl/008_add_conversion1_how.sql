-- Create Conversion 1 How table for context-linked processing metadata and generated codes.
CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.conversion1_how` (
  conversion1_how_code STRING NOT NULL,
  context_code STRING NOT NULL,
  process_code STRING,
  processing_code STRING NOT NULL,
  failure_mode STRING,
  machine_setup_url STRING,
  processed_data_url STRING,
  created_at TIMESTAMP NOT NULL,
  created_by STRING,
  updated_at TIMESTAMP,
  updated_by STRING,
  is_active BOOL NOT NULL
);

-- If the table already exists from an earlier schema, ensure required columns exist.
ALTER TABLE `PROJECT_ID.DATASET_ID.conversion1_how`
ADD COLUMN IF NOT EXISTS conversion1_how_code STRING;

ALTER TABLE `PROJECT_ID.DATASET_ID.conversion1_how`
ADD COLUMN IF NOT EXISTS processing_code STRING;

ALTER TABLE `PROJECT_ID.DATASET_ID.conversion1_how`
ADD COLUMN IF NOT EXISTS machine_setup_url STRING;

ALTER TABLE `PROJECT_ID.DATASET_ID.conversion1_how`
ADD COLUMN IF NOT EXISTS processed_data_url STRING;
