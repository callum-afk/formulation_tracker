from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from google.cloud import bigquery


@dataclass
class BigQueryService:
    project_id: str
    dataset_id: str

    def __post_init__(self) -> None:
        self.client = bigquery.Client(project=self.project_id)

    @property
    def dataset(self) -> str:
        return f"{self.project_id}.{self.dataset_id}"

    def _run(self, query: str, params: Sequence[bigquery.ScalarQueryParameter]) -> bigquery.job.QueryJob:
        job_config = bigquery.QueryJobConfig(query_parameters=list(params))
        return self.client.query(query, job_config=job_config)

    def allocate_counter(self, counter_name: str, scope: str, start_value: int) -> int:
        for _ in range(10):
            select_query = (
                f"SELECT next_value FROM `{self.dataset}.code_counters` "
                "WHERE counter_name = @counter_name AND scope = @scope"
            )
            select_job = self._run(
                select_query,
                [
                    bigquery.ScalarQueryParameter("counter_name", "STRING", counter_name),
                    bigquery.ScalarQueryParameter("scope", "STRING", scope),
                ],
            )
            rows = list(select_job.result())
            if not rows:
                insert_query = (
                    f"INSERT `{self.dataset}.code_counters` (counter_name, scope, next_value, updated_at) "
                    "VALUES (@counter_name, @scope, @next_value, CURRENT_TIMESTAMP())"
                )
                insert_job = self._run(
                    insert_query,
                    [
                        bigquery.ScalarQueryParameter("counter_name", "STRING", counter_name),
                        bigquery.ScalarQueryParameter("scope", "STRING", scope),
                        bigquery.ScalarQueryParameter("next_value", "INT64", start_value),
                    ],
                )
                insert_job.result()
                current_value = start_value
            else:
                current_value = int(rows[0]["next_value"])

            update_query = (
                f"UPDATE `{self.dataset}.code_counters` "
                "SET next_value = @next_value, updated_at = CURRENT_TIMESTAMP() "
                "WHERE counter_name = @counter_name AND scope = @scope AND next_value = @current_value"
            )
            update_job = self._run(
                update_query,
                [
                    bigquery.ScalarQueryParameter("next_value", "INT64", current_value + 1),
                    bigquery.ScalarQueryParameter("counter_name", "STRING", counter_name),
                    bigquery.ScalarQueryParameter("scope", "STRING", scope),
                    bigquery.ScalarQueryParameter("current_value", "INT64", current_value),
                ],
            )
            update_job.result()
            if update_job.num_dml_affected_rows == 1:
                return current_value
        raise RuntimeError("Failed to allocate counter after retries")

    def insert_ingredient(self, ingredient: Dict[str, Any]) -> None:
        query = (
            f"INSERT `{self.dataset}.ingredients` "
            "(sku, category_code, seq, pack_size_value, pack_size_unit, trade_name_inci, supplier, spec_grade, "
            "format, created_at, updated_at, created_by, updated_by, is_active, msds_object_path, msds_filename, msds_content_type, msds_uploaded_at) "
            "VALUES (@sku, @category_code, @seq, @pack_size_value, @pack_size_unit, @trade_name_inci, "
            "@supplier, @spec_grade, @format, @created_at, @updated_at, @created_by, @updated_by, "
            "@is_active, @msds_object_path, @msds_filename, @msds_content_type, @msds_uploaded_at)"
        )
        params = [
            bigquery.ScalarQueryParameter("sku", "STRING", ingredient["sku"]),
            bigquery.ScalarQueryParameter("category_code", "INT64", ingredient["category_code"]),
            bigquery.ScalarQueryParameter("seq", "INT64", ingredient["seq"]),
            bigquery.ScalarQueryParameter("pack_size_value", "INT64", ingredient["pack_size_value"]),
            bigquery.ScalarQueryParameter("pack_size_unit", "STRING", ingredient["pack_size_unit"]),
            bigquery.ScalarQueryParameter("trade_name_inci", "STRING", ingredient["trade_name_inci"]),
            bigquery.ScalarQueryParameter("supplier", "STRING", ingredient["supplier"]),
            bigquery.ScalarQueryParameter("spec_grade", "STRING", ingredient.get("spec_grade")),
            bigquery.ScalarQueryParameter("format", "STRING", ingredient["format"]),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", ingredient["created_at"]),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", ingredient["updated_at"]),
            bigquery.ScalarQueryParameter("created_by", "STRING", ingredient.get("created_by")),
            bigquery.ScalarQueryParameter("updated_by", "STRING", ingredient.get("updated_by")),
            bigquery.ScalarQueryParameter("is_active", "BOOL", ingredient["is_active"]),
            bigquery.ScalarQueryParameter("msds_object_path", "STRING", ingredient.get("msds_object_path")),
            bigquery.ScalarQueryParameter("msds_filename", "STRING", ingredient.get("msds_filename")),
            bigquery.ScalarQueryParameter("msds_content_type", "STRING", ingredient.get("msds_content_type")),
            bigquery.ScalarQueryParameter("msds_uploaded_at", "TIMESTAMP", ingredient.get("msds_uploaded_at")),
        ]
        self._run(query, params).result()

    def find_ingredient_duplicate(
        self,
        category_code: int,
        trade_name_inci: str,
        supplier: str,
        spec_grade: Optional[str],
        format: str,
        pack_size_value: int,
        pack_size_unit: str,
    ) -> Optional[Dict[str, Any]]:
        query = (
            f"SELECT * FROM `{self.dataset}.ingredients` "
            "WHERE category_code = @category_code "
            "AND trade_name_inci = @trade_name_inci "
            "AND supplier = @supplier "
            "AND format = @format "
            "AND pack_size_value = @pack_size_value "
            "AND pack_size_unit = @pack_size_unit "
            "AND ((spec_grade IS NULL AND @spec_grade IS NULL) OR spec_grade = @spec_grade) "
            "LIMIT 1"
        )
        params = [
            bigquery.ScalarQueryParameter("category_code", "INT64", category_code),
            bigquery.ScalarQueryParameter("trade_name_inci", "STRING", trade_name_inci),
            bigquery.ScalarQueryParameter("supplier", "STRING", supplier),
            bigquery.ScalarQueryParameter("format", "STRING", format),
            bigquery.ScalarQueryParameter("pack_size_value", "INT64", pack_size_value),
            bigquery.ScalarQueryParameter("pack_size_unit", "STRING", pack_size_unit),
            bigquery.ScalarQueryParameter("spec_grade", "STRING", spec_grade),
        ]
        rows = self._run(query, params).result()
        for row in rows:
            return dict(row)
        return None

    def find_ingredient_product(
        self,
        category_code: int,
        trade_name_inci: str,
        supplier: str,
        spec_grade: Optional[str],
        format: str,
        pack_size_unit: str,
    ) -> Optional[Dict[str, Any]]:
        query = (
            f"SELECT * FROM `{self.dataset}.ingredients` "
            "WHERE category_code = @category_code "
            "AND trade_name_inci = @trade_name_inci "
            "AND supplier = @supplier "
            "AND format = @format "
            "AND pack_size_unit = @pack_size_unit "
            "AND ((spec_grade IS NULL AND @spec_grade IS NULL) OR spec_grade = @spec_grade) "
            "LIMIT 1"
        )
        params = [
            bigquery.ScalarQueryParameter("category_code", "INT64", category_code),
            bigquery.ScalarQueryParameter("trade_name_inci", "STRING", trade_name_inci),
            bigquery.ScalarQueryParameter("supplier", "STRING", supplier),
            bigquery.ScalarQueryParameter("format", "STRING", format),
            bigquery.ScalarQueryParameter("pack_size_unit", "STRING", pack_size_unit),
            bigquery.ScalarQueryParameter("spec_grade", "STRING", spec_grade),
        ]
        rows = self._run(query, params).result()
        for row in rows:
            return dict(row)
        return None

    def find_ingredient_by_seq(self, seq: int) -> Optional[Dict[str, Any]]:
        query = f"SELECT * FROM `{self.dataset}.ingredients` WHERE seq = @seq LIMIT 1"
        rows = self._run(query, [bigquery.ScalarQueryParameter("seq", "INT64", seq)]).result()
        for row in rows:
            return dict(row)
        return None

    def list_ingredients(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        where = []
        params: List[bigquery.ScalarQueryParameter] = []
        if "q" in filters:
            where.append(
                "(sku LIKE @q OR trade_name_inci LIKE @q OR supplier LIKE @q)"
            )
            params.append(bigquery.ScalarQueryParameter("q", "STRING", f"%{filters['q']}%"))
        for field in ("category_code", "format", "pack_size_unit", "is_active"):
            if field in filters:
                where.append(f"{field} = @{field}")
                params.append(bigquery.ScalarQueryParameter(field, "STRING" if isinstance(filters[field], str) else "INT64", filters[field]))
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""
        query = f"SELECT * FROM `{self.dataset}.ingredients` {where_clause} ORDER BY sku"
        rows = self._run(query, params).result()
        return [dict(row) for row in rows]

    def get_ingredient(self, sku: str) -> Optional[Dict[str, Any]]:
        query = f"SELECT * FROM `{self.dataset}.ingredients` WHERE sku = @sku"
        rows = self._run(query, [bigquery.ScalarQueryParameter("sku", "STRING", sku)]).result()
        for row in rows:
            return dict(row)
        return None

    def update_msds(self, sku: str, object_path: str, filename: str, content_type: str, updated_by: str | None = None) -> None:
        query = (
            f"UPDATE `{self.dataset}.ingredients` "
            "SET msds_object_path = @object_path, msds_filename = @filename, msds_content_type = @content_type, msds_uploaded_at = CURRENT_TIMESTAMP(), "
            "updated_at = CURRENT_TIMESTAMP(), updated_by = @updated_by "
            "WHERE sku = @sku"
        )
        self._run(
            query,
            [
                bigquery.ScalarQueryParameter("object_path", "STRING", object_path),
                bigquery.ScalarQueryParameter("filename", "STRING", filename),
                bigquery.ScalarQueryParameter("content_type", "STRING", content_type),
                bigquery.ScalarQueryParameter("updated_by", "STRING", updated_by),
                bigquery.ScalarQueryParameter("sku", "STRING", sku),
            ],
        ).result()

    def insert_batch(self, batch: Dict[str, Any]) -> None:
        query = (
            f"INSERT `{self.dataset}.ingredient_batches` "
            "(sku, ingredient_batch_code, received_at, notes, quantity_value, quantity_unit, created_at, updated_at, created_by, updated_by, "
            "is_active, spec_object_path, spec_uploaded_at) "
            "VALUES (@sku, @ingredient_batch_code, @received_at, @notes, @quantity_value, @quantity_unit, @created_at, @updated_at, "
            "@created_by, @updated_by, @is_active, @spec_object_path, @spec_uploaded_at)"
        )
        params = [
            bigquery.ScalarQueryParameter("sku", "STRING", batch["sku"]),
            bigquery.ScalarQueryParameter("ingredient_batch_code", "STRING", batch["ingredient_batch_code"]),
            bigquery.ScalarQueryParameter("received_at", "TIMESTAMP", batch.get("received_at")),
            bigquery.ScalarQueryParameter("notes", "STRING", batch.get("notes")),
            bigquery.ScalarQueryParameter("quantity_value", "FLOAT64", batch.get("quantity_value")),
            bigquery.ScalarQueryParameter("quantity_unit", "STRING", batch.get("quantity_unit")),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", batch["created_at"]),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", batch["updated_at"]),
            bigquery.ScalarQueryParameter("created_by", "STRING", batch.get("created_by")),
            bigquery.ScalarQueryParameter("updated_by", "STRING", batch.get("updated_by")),
            bigquery.ScalarQueryParameter("is_active", "BOOL", batch["is_active"]),
            bigquery.ScalarQueryParameter("spec_object_path", "STRING", batch.get("spec_object_path")),
            bigquery.ScalarQueryParameter("spec_uploaded_at", "TIMESTAMP", batch.get("spec_uploaded_at")),
        ]
        self._run(query, params).result()

    def list_batches(self, sku: str) -> List[Dict[str, Any]]:
        query = (
            f"SELECT * FROM `{self.dataset}.ingredient_batches` "
            "WHERE sku = @sku ORDER BY ingredient_batch_code"
        )
        rows = self._run(query, [bigquery.ScalarQueryParameter("sku", "STRING", sku)]).result()
        return [dict(row) for row in rows]

    def get_batch(self, sku: str, batch_code: str) -> Optional[Dict[str, Any]]:
        query = (
            f"SELECT * FROM `{self.dataset}.ingredient_batches` "
            "WHERE sku = @sku AND ingredient_batch_code = @batch_code LIMIT 1"
        )
        rows = self._run(
            query,
            [
                bigquery.ScalarQueryParameter("sku", "STRING", sku),
                bigquery.ScalarQueryParameter("batch_code", "STRING", batch_code),
            ],
        ).result()
        for row in rows:
            return dict(row)
        return None

    def update_spec(self, sku: str, batch_code: str, object_path: str) -> None:
        query = (
            f"UPDATE `{self.dataset}.ingredient_batches` "
            "SET spec_object_path = @object_path, spec_uploaded_at = CURRENT_TIMESTAMP(), updated_at = CURRENT_TIMESTAMP() "
            "WHERE sku = @sku AND ingredient_batch_code = @batch_code"
        )
        self._run(
            query,
            [
                bigquery.ScalarQueryParameter("object_path", "STRING", object_path),
                bigquery.ScalarQueryParameter("sku", "STRING", sku),
                bigquery.ScalarQueryParameter("batch_code", "STRING", batch_code),
            ],
        ).result()

    def get_set_by_hash(self, set_hash: str) -> Optional[str]:
        query = f"SELECT set_code FROM `{self.dataset}.ingredient_sets` WHERE set_hash = @set_hash"
        rows = self._run(query, [bigquery.ScalarQueryParameter("set_hash", "STRING", set_hash)]).result()
        for row in rows:
            return row["set_code"]
        return None

    def insert_set(self, set_code: str, set_hash: str, skus: Iterable[str], created_by: Optional[str]) -> None:
        now = datetime.now(timezone.utc)
        insert_set_query = (
            f"INSERT `{self.dataset}.ingredient_sets` (set_code, set_hash, created_at, created_by) "
            "VALUES (@set_code, @set_hash, @created_at, @created_by)"
        )
        self._run(
            insert_set_query,
            [
                bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                bigquery.ScalarQueryParameter("set_hash", "STRING", set_hash),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
            ],
        ).result()
        insert_items_query = (
            f"INSERT `{self.dataset}.ingredient_set_items` (set_code, sku, created_at, created_by) "
            "VALUES (@set_code, @sku, @created_at, @created_by)"
        )
        for sku in skus:
            self._run(
                insert_items_query,
                [
                    bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                    bigquery.ScalarQueryParameter("sku", "STRING", sku),
                    bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now),
                    bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
                ],
            ).result()

    def list_sets(self) -> List[Dict[str, Any]]:
        query = f"SELECT * FROM `{self.dataset}.v_sets` ORDER BY set_code"
        rows = self._run(query, []).result()
        return [dict(row) for row in rows]

    def get_set(self, set_code: str) -> Optional[Dict[str, Any]]:
        query = f"SELECT * FROM `{self.dataset}.v_sets` WHERE set_code = @set_code LIMIT 1"
        rows = self._run(query, [bigquery.ScalarQueryParameter("set_code", "STRING", set_code)]).result()
        for row in rows:
            return dict(row)
        return None

    def get_weight_by_hash(self, set_code: str, weight_hash: str) -> Optional[str]:
        query = (
            f"SELECT weight_code FROM `{self.dataset}.dry_weight_variants` "
            "WHERE set_code = @set_code AND weight_hash = @weight_hash"
        )
        rows = self._run(
            query,
            [
                bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                bigquery.ScalarQueryParameter("weight_hash", "STRING", weight_hash),
            ],
        ).result()
        for row in rows:
            return row["weight_code"]
        return None

    def insert_weight_variant(
        self,
        set_code: str,
        weight_code: str,
        weight_hash: str,
        items: Iterable[Tuple[str, float]],
        created_by: Optional[str],
        notes: Optional[str] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        insert_variant_query = (
            f"INSERT `{self.dataset}.dry_weight_variants` "
            "(set_code, weight_code, weight_hash, created_at, created_by, notes) "
            "VALUES (@set_code, @weight_code, @weight_hash, @created_at, @created_by, @notes)"
        )
        self._run(
            insert_variant_query,
            [
                bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                bigquery.ScalarQueryParameter("weight_code", "STRING", weight_code),
                bigquery.ScalarQueryParameter("weight_hash", "STRING", weight_hash),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
                bigquery.ScalarQueryParameter("notes", "STRING", notes),
            ],
        ).result()
        insert_items_query = (
            f"INSERT `{self.dataset}.dry_weight_items` "
            "(set_code, weight_code, sku, wt_percent, created_at, created_by) "
            "VALUES (@set_code, @weight_code, @sku, @wt_percent, @created_at, @created_by)"
        )
        for sku, wt in items:
            self._run(
                insert_items_query,
                [
                    bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                    bigquery.ScalarQueryParameter("weight_code", "STRING", weight_code),
                    bigquery.ScalarQueryParameter("sku", "STRING", sku),
                    bigquery.ScalarQueryParameter("wt_percent", "NUMERIC", wt),
                    bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now),
                    bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
                ],
            ).result()

    def list_weights(self, set_code: str) -> List[Dict[str, Any]]:
        query = (
            f"SELECT * FROM `{self.dataset}.v_weight_variants` "
            "WHERE set_code = @set_code ORDER BY weight_code"
        )
        rows = self._run(
            query,
            [bigquery.ScalarQueryParameter("set_code", "STRING", set_code)],
        ).result()
        return [dict(row) for row in rows]

    def get_weight(self, set_code: str, weight_code: str) -> Optional[Dict[str, Any]]:
        query = (
            f"SELECT * FROM `{self.dataset}.v_weight_variants` "
            "WHERE set_code = @set_code AND weight_code = @weight_code LIMIT 1"
        )
        rows = self._run(
            query,
            [
                bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                bigquery.ScalarQueryParameter("weight_code", "STRING", weight_code),
            ],
        ).result()
        for row in rows:
            return dict(row)
        return None

    def get_batch_variant_by_hash(self, set_code: str, weight_code: str, batch_hash: str) -> Optional[str]:
        query = (
            f"SELECT batch_variant_code FROM `{self.dataset}.batch_variants` "
            "WHERE set_code = @set_code AND weight_code = @weight_code AND batch_hash = @batch_hash"
        )
        rows = self._run(
            query,
            [
                bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                bigquery.ScalarQueryParameter("weight_code", "STRING", weight_code),
                bigquery.ScalarQueryParameter("batch_hash", "STRING", batch_hash),
            ],
        ).result()
        for row in rows:
            return row["batch_variant_code"]
        return None

    def insert_batch_variant(
        self,
        set_code: str,
        weight_code: str,
        batch_variant_code: str,
        batch_hash: str,
        items: Iterable[Tuple[str, str]],
        created_by: Optional[str],
        notes: Optional[str] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        insert_variant_query = (
            f"INSERT `{self.dataset}.batch_variants` "
            "(set_code, weight_code, batch_variant_code, batch_hash, created_at, created_by, notes) "
            "VALUES (@set_code, @weight_code, @batch_variant_code, @batch_hash, @created_at, @created_by, @notes)"
        )
        self._run(
            insert_variant_query,
            [
                bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                bigquery.ScalarQueryParameter("weight_code", "STRING", weight_code),
                bigquery.ScalarQueryParameter("batch_variant_code", "STRING", batch_variant_code),
                bigquery.ScalarQueryParameter("batch_hash", "STRING", batch_hash),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
                bigquery.ScalarQueryParameter("notes", "STRING", notes),
            ],
        ).result()
        insert_items_query = (
            f"INSERT `{self.dataset}.batch_variant_items` "
            "(set_code, weight_code, batch_variant_code, sku, ingredient_batch_code, created_at, created_by) "
            "VALUES (@set_code, @weight_code, @batch_variant_code, @sku, @ingredient_batch_code, @created_at, @created_by)"
        )
        for sku, batch_code in items:
            self._run(
                insert_items_query,
                [
                    bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                    bigquery.ScalarQueryParameter("weight_code", "STRING", weight_code),
                    bigquery.ScalarQueryParameter("batch_variant_code", "STRING", batch_variant_code),
                    bigquery.ScalarQueryParameter("sku", "STRING", sku),
                    bigquery.ScalarQueryParameter("ingredient_batch_code", "STRING", batch_code),
                    bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now),
                    bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
                ],
            ).result()

    def list_batch_variants(self, set_code: str, weight_code: str) -> List[Dict[str, Any]]:
        query = (
            f"SELECT * FROM `{self.dataset}.v_batch_variants` "
            "WHERE set_code = @set_code AND weight_code = @weight_code ORDER BY batch_variant_code"
        )
        rows = self._run(
            query,
            [
                bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                bigquery.ScalarQueryParameter("weight_code", "STRING", weight_code),
            ],
        ).result()
        return [dict(row) for row in rows]

    def list_formulations(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        where = []
        params: List[bigquery.ScalarQueryParameter] = []
        # Apply exact-match filters that map directly to formulation columns.
        for field in ("set_code", "weight_code", "batch_variant_code"):
            if field in filters:
                where.append(f"f.{field} = @{field}")
                params.append(bigquery.ScalarQueryParameter(field, "STRING", filters[field]))
        # Apply a SKU containment filter by searching the JSON-encoded SKU list payload.
        if "sku" in filters:
            where.append("CONTAINS_SUBSTR(TO_JSON_STRING(f.sku_list), @sku)")
            params.append(bigquery.ScalarQueryParameter("sku", "STRING", filters["sku"]))
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""
        query = (
            f"SELECT f.* FROM `{self.dataset}.v_formulations_flat` f "
            f"{where_clause} ORDER BY f.created_at DESC"
        )
        rows = self._run(query, params).result()
        return [dict(row) for row in rows]

    def list_formulations_by_batch(self, sku: str, batch_code: str) -> List[Dict[str, Any]]:
        # Find every formulation where the nested batch items include the requested SKU + batch code pair.
        query = (
            f"SELECT f.* FROM `{self.dataset}.v_formulations_flat` f "
            "WHERE EXISTS ("
            "  SELECT 1 FROM UNNEST(f.batch_items) AS batch_item "
            "  WHERE batch_item.sku = @sku AND batch_item.ingredient_batch_code = @batch_code"
            ") "
            "ORDER BY f.created_at DESC"
        )
        rows = self._run(
            query,
            [
                bigquery.ScalarQueryParameter("sku", "STRING", sku),
                bigquery.ScalarQueryParameter("batch_code", "STRING", batch_code),
            ],
        ).result()
        return [dict(row) for row in rows]
