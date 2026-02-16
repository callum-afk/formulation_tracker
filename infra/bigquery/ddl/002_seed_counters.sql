MERGE `PROJECT_ID.DATASET_ID.code_counters` T
USING (SELECT 'set_code' AS counter_name, '' AS scope, 1 AS next_value) S
ON T.counter_name = S.counter_name AND T.scope = S.scope
WHEN NOT MATCHED THEN
  INSERT (counter_name, scope, next_value, updated_at)
  VALUES (S.counter_name, S.scope, S.next_value, CURRENT_TIMESTAMP());

MERGE `PROJECT_ID.DATASET_ID.code_counters` T
USING (SELECT 'location_partner_code' AS counter_name, '' AS scope, 31 AS next_value) S
ON T.counter_name = S.counter_name AND T.scope = S.scope
WHEN NOT MATCHED THEN
  INSERT (counter_name, scope, next_value, updated_at)
  VALUES (S.counter_name, S.scope, S.next_value, CURRENT_TIMESTAMP());
