CREATE OR REPLACE VIEW `PROJECT_ID.DATASET_ID.v_sets` AS
SELECT
  s.set_code,
  s.set_hash,
  s.created_at,
  s.created_by,
  ARRAY_AGG(i.sku ORDER BY i.sku) AS sku_list
FROM `PROJECT_ID.DATASET_ID.ingredient_sets` s
JOIN `PROJECT_ID.DATASET_ID.ingredient_set_items` i
ON i.set_code = s.set_code
GROUP BY s.set_code, s.set_hash, s.created_at, s.created_by;

CREATE OR REPLACE VIEW `PROJECT_ID.DATASET_ID.v_weight_variants` AS
SELECT
  v.set_code,
  v.weight_code,
  v.weight_hash,
  v.created_at,
  v.created_by,
  ARRAY_AGG(STRUCT(i.sku AS sku, i.wt_percent AS wt_percent) ORDER BY i.sku) AS items
FROM `PROJECT_ID.DATASET_ID.dry_weight_variants` v
JOIN `PROJECT_ID.DATASET_ID.dry_weight_items` i
ON i.set_code = v.set_code AND i.weight_code = v.weight_code
GROUP BY v.set_code, v.weight_code, v.weight_hash, v.created_at, v.created_by;

CREATE OR REPLACE VIEW `PROJECT_ID.DATASET_ID.v_batch_variants` AS
SELECT
  v.set_code,
  v.weight_code,
  v.batch_variant_code,
  v.batch_hash,
  v.created_at,
  ANY_VALUE(v.created_by) AS created_by,
  ARRAY_AGG(
    STRUCT(i.sku AS sku, i.ingredient_batch_code AS ingredient_batch_code)
    ORDER BY i.sku
  ) AS items
FROM `PROJECT_ID.DATASET_ID.batch_variants` v
JOIN `PROJECT_ID.DATASET_ID.batch_variant_items` i
ON i.set_code = v.set_code AND i.weight_code = v.weight_code AND i.batch_variant_code = v.batch_variant_code
GROUP BY v.set_code, v.weight_code, v.batch_variant_code, v.batch_hash, v.created_at, v.created_by;

CREATE OR REPLACE VIEW `PROJECT_ID.DATASET_ID.v_formulations_flat` AS
SELECT
  b.set_code,
  b.weight_code,
  b.batch_variant_code,
  CONCAT(b.set_code, ' ', b.weight_code, ' ', b.batch_variant_code) AS base_code,
  b.created_at,
  b.created_by,
  b.sku_list,
  ARRAY_LENGTH(b.sku_list) AS sku_count,
  b.batch_items,
  ARRAY(
    SELECT AS STRUCT
      formulation_sku AS sku,
      (
        SELECT wi.wt_percent
        FROM UNNEST(w.items) wi
        WHERE wi.sku = formulation_sku
        LIMIT 1
      ) AS wt_percent
    FROM UNNEST(b.sku_list) AS formulation_sku
  ) AS dry_weight_items
FROM (
  SELECT
    v.set_code,
    v.weight_code,
    v.batch_variant_code,
    v.created_at,
    v.created_by,
    ARRAY(SELECT x.sku FROM UNNEST(v.items) x ORDER BY x.sku) AS sku_list,
    ARRAY(
      SELECT AS STRUCT x.sku AS sku, x.ingredient_batch_code AS ingredient_batch_code
      FROM UNNEST(v.items) x
      ORDER BY x.sku
    ) AS batch_items
  FROM `PROJECT_ID.DATASET_ID.v_batch_variants` v
) b
LEFT JOIN `PROJECT_ID.DATASET_ID.v_weight_variants` w
ON w.set_code = b.set_code AND w.weight_code = b.weight_code;
