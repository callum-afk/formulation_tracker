CREATE OR REPLACE VIEW `PROJECT_ID.DATASET_ID.v_sets` AS
SELECT
  s.set_code,
  s.set_hash,
  s.created_at,
  ARRAY_AGG(i.sku ORDER BY i.sku) AS sku_list
FROM `PROJECT_ID.DATASET_ID.ingredient_sets` s
JOIN `PROJECT_ID.DATASET_ID.ingredient_set_items` i
ON i.set_code = s.set_code
GROUP BY s.set_code, s.set_hash, s.created_at;

CREATE OR REPLACE VIEW `PROJECT_ID.DATASET_ID.v_weight_variants` AS
SELECT
  v.set_code,
  v.weight_code,
  v.weight_hash,
  v.created_at,
  ARRAY_AGG(STRUCT(i.sku AS sku, i.wt_percent AS wt_percent) ORDER BY i.sku) AS items
FROM `PROJECT_ID.DATASET_ID.dry_weight_variants` v
JOIN `PROJECT_ID.DATASET_ID.dry_weight_items` i
ON i.set_code = v.set_code AND i.weight_code = v.weight_code
GROUP BY v.set_code, v.weight_code, v.weight_hash, v.created_at;

CREATE OR REPLACE VIEW `PROJECT_ID.DATASET_ID.v_batch_variants` AS
SELECT
  v.set_code,
  v.weight_code,
  v.batch_variant_code,
  v.batch_hash,
  v.created_at,
  ARRAY_AGG(
    STRUCT(i.sku AS sku, i.ingredient_batch_code AS ingredient_batch_code)
    ORDER BY i.sku
  ) AS items
FROM `PROJECT_ID.DATASET_ID.batch_variants` v
JOIN `PROJECT_ID.DATASET_ID.batch_variant_items` i
ON i.set_code = v.set_code AND i.weight_code = v.weight_code AND i.batch_variant_code = v.batch_variant_code
GROUP BY v.set_code, v.weight_code, v.batch_variant_code, v.batch_hash, v.created_at;

CREATE OR REPLACE VIEW `PROJECT_ID.DATASET_ID.v_formulations_flat` AS
SELECT
  b.set_code,
  b.weight_code,
  b.batch_variant_code,
  CONCAT(b.set_code, ' ', b.weight_code, ' ', b.batch_variant_code) AS base_code,
  b.created_at,
  ARRAY_LENGTH(b.items) AS sku_count,
  ARRAY(SELECT x.sku FROM UNNEST(b.items) x ORDER BY x.sku) AS sku_list,
  b.items AS batch_items
FROM `PROJECT_ID.DATASET_ID.v_batch_variants` b;
