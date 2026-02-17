CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.compounding_how` (
  processing_code STRING NOT NULL,
  location_code STRING NOT NULL,
  process_code_suffix STRING NOT NULL,
  failure_mode STRING NOT NULL,
  machine_setup_url STRING,
  processed_data_url STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  created_by STRING,
  updated_by STRING,
  is_active BOOL NOT NULL
);

MERGE `PROJECT_ID.DATASET_ID.code_counters` AS target
USING (
  SELECT 'compounding_process_code' AS counter_name, '' AS scope, 1 AS next_value
) AS source
ON target.counter_name = source.counter_name AND target.scope = source.scope
WHEN NOT MATCHED THEN
  INSERT (counter_name, scope, next_value, updated_at)
  VALUES (source.counter_name, source.scope, source.next_value, CURRENT_TIMESTAMP());
