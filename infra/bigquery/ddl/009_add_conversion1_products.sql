-- Create Conversion 1 products table for globally sequenced product-code rows.
CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.conversion1_products` (
  product_code STRING NOT NULL,
  conversion1_how_code STRING NOT NULL,
  product_suffix STRING NOT NULL,
  storage_location STRING,
  notes STRING,
  number_units_produced INT64,
  numbered_in_order BOOL,
  tensile_rigid_status STRING,
  tensile_films_status STRING,
  seal_strength_status STRING,
  shelf_stability_status STRING,
  solubility_status STRING,
  defect_analysis_status STRING,
  blocking_status STRING,
  film_emc_status STRING,
  friction_status STRING,
  width_mm INT64,
  length_m INT64,
  avg_film_thickness_um INT64,
  sd_film_thickness FLOAT64,
  film_thickness_variation_percent FLOAT64,
  created_at TIMESTAMP NOT NULL,
  created_by STRING,
  updated_at TIMESTAMP,
  updated_by STRING,
  is_active BOOL NOT NULL
);

-- Create dedicated single-row counter table used to atomically reserve product suffix ranges.
CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.conversion1_product_counter` (
  id STRING NOT NULL,
  next_suffix INT64 NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
