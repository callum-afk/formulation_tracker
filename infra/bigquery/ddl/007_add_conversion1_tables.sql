CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.conversion1_context` (
  context_code STRING NOT NULL,
  pellet_bag_code STRING NOT NULL,
  partner_code STRING NOT NULL,
  machine_code STRING NOT NULL,
  date_yymmdd STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING,
  updated_at TIMESTAMP NOT NULL,
  updated_by STRING,
  is_active BOOL NOT NULL
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.conversion1_how` (
  how_code STRING NOT NULL,
  context_code STRING NOT NULL,
  process_code STRING NOT NULL,
  process_id STRING NOT NULL,
  notes STRING,
  failure_mode STRING,
  setup_link STRING,
  processed_data_link STRING,
  created_at TIMESTAMP NOT NULL,
  created_by STRING,
  updated_at TIMESTAMP NOT NULL,
  updated_by STRING,
  is_active BOOL NOT NULL
);

MERGE `PROJECT_ID.DATASET_ID.code_counters` AS target
USING (
  SELECT 'conversion1_process_code' AS counter_name, '' AS scope, 1 AS next_value
) AS source
ON target.counter_name = source.counter_name AND target.scope = source.scope
WHEN NOT MATCHED THEN
  INSERT (counter_name, scope, next_value, updated_at)
  VALUES (source.counter_name, source.scope, source.next_value, CURRENT_TIMESTAMP());
