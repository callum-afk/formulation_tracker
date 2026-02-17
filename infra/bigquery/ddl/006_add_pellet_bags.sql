CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.pellet_bags` (
  pellet_bag_id STRING NOT NULL,
  pellet_bag_code STRING NOT NULL,
  pellet_bag_code_tokens ARRAY<STRING> NOT NULL,
  compounding_how_code STRING NOT NULL,
  product_type STRING NOT NULL,
  sequence_number INT64 NOT NULL,
  bag_mass_kg FLOAT64 NOT NULL,
  remaining_mass_kg FLOAT64 NOT NULL,
  short_moisture_percent FLOAT64,
  purpose STRING,
  reference_sample_taken STRING,
  qc_status STRING,
  long_moisture_status STRING,
  density_status STRING,
  injection_moulding_status STRING,
  film_forming_status STRING,
  injection_moulding_assignee_email STRING,
  film_forming_assignee_email STRING,
  notes STRING,
  customer STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  created_by STRING,
  updated_by STRING,
  is_active BOOL NOT NULL
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.pellet_bag_assignees` (
  email STRING NOT NULL,
  is_active BOOL NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING
);
