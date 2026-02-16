-- Backfill migration for environments that were provisioned before location-code tables were introduced.
CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.location_partners` (
  -- Two-letter partner identifier used in generated location IDs.
  partner_code STRING NOT NULL,
  -- Human-friendly partner name shown in dropdowns and tables.
  partner_name STRING NOT NULL,
  -- Machine/context descriptor to distinguish similarly named partner entries.
  machine_specification STRING,
  -- Audit timestamp for when the partner code row was created.
  created_at TIMESTAMP NOT NULL,
  -- User email or service identity that created the row.
  created_by STRING
);

-- Backfill migration for persisted generated location IDs.
CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.location_codes` (
  -- Set identifier component of the location ID.
  set_code STRING NOT NULL,
  -- Dry-weight identifier component of the location ID.
  weight_code STRING NOT NULL,
  -- Batch-variant identifier component of the location ID.
  batch_variant_code STRING NOT NULL,
  -- Partner identifier component of the location ID.
  partner_code STRING NOT NULL,
  -- YYMMDD production date code component.
  production_date STRING NOT NULL,
  -- Fully formatted location ID string.
  location_id STRING NOT NULL,
  -- Audit timestamp for row creation.
  created_at TIMESTAMP NOT NULL,
  -- User email or service identity that created the row.
  created_by STRING
);

-- Ensure partner-code counter exists and never starts below BF (31) so it does not collide with seeded AA-BE codes.
MERGE `PROJECT_ID.DATASET_ID.code_counters` AS target
USING (SELECT 'location_partner_code' AS counter_name, '' AS scope, 31 AS min_next_value) AS source
ON target.counter_name = source.counter_name AND target.scope = source.scope
WHEN MATCHED AND target.next_value < source.min_next_value THEN
  UPDATE SET target.next_value = source.min_next_value, target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (counter_name, scope, next_value, updated_at)
  VALUES (source.counter_name, source.scope, source.min_next_value, CURRENT_TIMESTAMP());
