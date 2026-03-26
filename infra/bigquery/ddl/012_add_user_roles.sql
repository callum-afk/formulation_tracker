CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.user_roles` (
  email STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  role_group STRING NOT NULL,
  permissions ARRAY<STRING>,
  is_active BOOL NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING,
  updated_at TIMESTAMP,
  updated_by STRING
);
