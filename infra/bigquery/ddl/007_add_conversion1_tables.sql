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
