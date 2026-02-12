CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.DATASET_ID`;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.ingredients` (
  sku STRING NOT NULL,
  category_code INT64 NOT NULL,
  seq INT64 NOT NULL,
  pack_size_value INT64 NOT NULL,
  pack_size_unit STRING NOT NULL,
  trade_name_inci STRING NOT NULL,
  supplier STRING NOT NULL,
  spec_grade STRING,
  format STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  created_by STRING,
  updated_by STRING,
  is_active BOOL NOT NULL,
  msds_object_path STRING,
  msds_filename STRING,
  msds_content_type STRING,
  msds_uploaded_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.ingredient_batches` (
  sku STRING NOT NULL,
  ingredient_batch_code STRING NOT NULL,
  received_at TIMESTAMP,
  notes STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  created_by STRING,
  updated_by STRING,
  is_active BOOL NOT NULL,
  spec_object_path STRING,
  spec_uploaded_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.ingredient_sets` (
  set_code STRING NOT NULL,
  set_hash STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.ingredient_set_items` (
  set_code STRING NOT NULL,
  sku STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.dry_weight_variants` (
  set_code STRING NOT NULL,
  weight_code STRING NOT NULL,
  weight_hash STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING,
  notes STRING
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.dry_weight_items` (
  set_code STRING NOT NULL,
  weight_code STRING NOT NULL,
  sku STRING NOT NULL,
  wt_percent NUMERIC NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.batch_variants` (
  set_code STRING NOT NULL,
  weight_code STRING NOT NULL,
  batch_variant_code STRING NOT NULL,
  batch_hash STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING,
  notes STRING
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.batch_variant_items` (
  set_code STRING NOT NULL,
  weight_code STRING NOT NULL,
  batch_variant_code STRING NOT NULL,
  sku STRING NOT NULL,
  ingredient_batch_code STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.code_counters` (
  counter_name STRING NOT NULL,
  scope STRING NOT NULL,
  next_value INT64 NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.audit_log` (
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,
  actor STRING,
  event_at TIMESTAMP NOT NULL,
  entity_type STRING NOT NULL,
  entity_id STRING NOT NULL,
  payload_json STRING
);
