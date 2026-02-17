from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
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
        # Force US location to avoid cross-region errors when this dataset is provisioned in US.
        job_config = bigquery.QueryJobConfig(query_parameters=list(params))
        job_config.location = "US"
        return self.client.query(query, job_config=job_config)

    def run_startup_sql(self) -> None:
        # Execute DDL migrations and view definitions in lexical order so deploys self-heal missing schema objects.
        sql_roots = [Path("infra/bigquery/ddl"), Path("infra/bigquery/views")]
        for root in sql_roots:
            if not root.exists():
                continue
            for sql_file in sorted(root.glob("*.sql")):
                sql = sql_file.read_text(encoding="utf-8")
                rendered = sql.replace("PROJECT_ID", self.project_id).replace("DATASET_ID", self.dataset_id)
                # Split on statement terminators so multi-statement migration files can run sequentially.
                for statement in (part.strip() for part in rendered.split(";") if part.strip()):
                    self._run(statement, []).result()

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
        # Keep the legacy SKU-specific listing helper for endpoints that need exact SKU scope.
        query = (
            f"SELECT * FROM `{self.dataset}.ingredient_batches` "
            "WHERE sku = @sku ORDER BY ingredient_batch_code"
        )
        rows = self._run(query, [bigquery.ScalarQueryParameter("sku", "STRING", sku)]).result()
        return [dict(row) for row in rows]

    def list_batches_paginated(
        self,
        sku: Optional[str],
        batch_code: Optional[str],
        page: int,
        page_size: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        # Build dynamic filter clauses so the same query can support all/sku/batch lookup combinations.
        where: List[str] = []
        params: List[bigquery.ScalarQueryParameter] = []
        if sku:
            where.append("sku = @sku")
            params.append(bigquery.ScalarQueryParameter("sku", "STRING", sku))
        if batch_code:
            where.append("ingredient_batch_code = @batch_code")
            params.append(bigquery.ScalarQueryParameter("batch_code", "STRING", batch_code))

        # Assemble the WHERE clause only when one or more optional filters are provided.
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""
        offset = max(page - 1, 0) * page_size

        # Count first so the UI can render correct total pages for the current filter set.
        count_query = f"SELECT COUNT(1) AS total FROM `{self.dataset}.ingredient_batches` {where_clause}"
        total_rows = list(self._run(count_query, params).result())
        total = int(total_rows[0]["total"]) if total_rows else 0

        # Return oldest-to-newest records and add deterministic tie-breakers for stable pagination.
        data_query = (
            f"SELECT * FROM `{self.dataset}.ingredient_batches` "
            f"{where_clause} "
            "ORDER BY created_at ASC, sku ASC, ingredient_batch_code ASC "
            "LIMIT @limit OFFSET @offset"
        )
        data_params = [
            *params,
            bigquery.ScalarQueryParameter("limit", "INT64", page_size),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
        rows = self._run(data_query, data_params).result()
        return [dict(row) for row in rows], total

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

    def list_sets_paginated(self, search: Optional[str], page: int, page_size: int) -> Tuple[List[Dict[str, Any]], int]:
        # Support set lookup by set code or contained SKU while keeping a single source of query truth.
        where: List[str] = []
        params: List[bigquery.ScalarQueryParameter] = []
        if search:
            where.append(
                "(CONTAINS_SUBSTR(set_code, @search) "
                "OR EXISTS (SELECT 1 FROM UNNEST(sku_list) AS sku WHERE CONTAINS_SUBSTR(sku, @search)))"
            )
            params.append(bigquery.ScalarQueryParameter("search", "STRING", search))

        # Apply optional filtering and compute the offset for 1-indexed page navigation.
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""
        offset = max(page - 1, 0) * page_size

        # Count rows for pagination controls so the frontend can show page totals accurately.
        count_query = f"SELECT COUNT(1) AS total FROM `{self.dataset}.v_sets` {where_clause}"
        total_rows = list(self._run(count_query, params).result())
        total = int(total_rows[0]["total"]) if total_rows else 0

        # Default ordering is oldest-to-newest, with set_code as a deterministic tie-breaker.
        data_query = (
            f"SELECT * FROM `{self.dataset}.v_sets` "
            f"{where_clause} "
            "ORDER BY created_at ASC, set_code ASC "
            "LIMIT @limit OFFSET @offset"
        )
        data_params = [
            *params,
            bigquery.ScalarQueryParameter("limit", "INT64", page_size),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
        rows = self._run(data_query, data_params).result()
        return [dict(row) for row in rows], total

    def list_sets(self) -> List[Dict[str, Any]]:
        # Preserve existing method behavior for any legacy callers that still require a full set list.
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

    def list_formulations_paginated(
        self,
        filters: Dict[str, Any],
        page: int,
        page_size: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
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
        # Build a count query for accurate page controls under all active filter combinations.
        count_query = f"SELECT COUNT(1) AS total FROM `{self.dataset}.v_formulations_flat` f {where_clause}"
        total_rows = list(self._run(count_query, params).result())
        total = int(total_rows[0]["total"]) if total_rows else 0

        # Keep newest-to-oldest sort and include extra tie-breakers so paging is deterministic.
        query = (
            f"SELECT f.* FROM `{self.dataset}.v_formulations_flat` f "
            f"{where_clause} "
            "ORDER BY f.created_at DESC, f.set_code DESC, f.weight_code DESC, f.batch_variant_code DESC "
            "LIMIT @limit OFFSET @offset"
        )
        offset = max(page - 1, 0) * page_size
        data_params = [
            *params,
            bigquery.ScalarQueryParameter("limit", "INT64", page_size),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
        rows = self._run(query, data_params).result()
        return [dict(row) for row in rows], total

    def list_formulations(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Preserve pre-pagination method for backward compatibility.
        rows, _ = self.list_formulations_paginated(filters=filters, page=1, page_size=1000)
        return rows

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

    def list_location_partners(self) -> List[Dict[str, Any]]:
        # Return all persisted custom partner-code mappings in code order for predictable dropdown rendering.
        query = (
            f"SELECT partner_code, partner_name, machine_specification, created_at, created_by "
            f"FROM `{self.dataset}.location_partners` ORDER BY partner_code"
        )
        rows = self._run(query, []).result()
        return [dict(row) for row in rows]

    def get_location_partner(self, partner_code: str) -> Optional[Dict[str, Any]]:
        # Fetch a single custom location partner row by its two-letter partner code.
        query = (
            f"SELECT partner_code, partner_name, machine_specification, created_at, created_by "
            f"FROM `{self.dataset}.location_partners` WHERE partner_code = @partner_code LIMIT 1"
        )
        rows = self._run(
            query,
            [bigquery.ScalarQueryParameter("partner_code", "STRING", partner_code)],
        ).result()
        for row in rows:
            return dict(row)
        return None

    def insert_location_partner(
        self,
        partner_code: str,
        partner_name: str,
        machine_specification: str,
        created_by: Optional[str],
    ) -> None:
        # Persist a newly created custom partner and machine specification to support future selection.
        query = (
            f"INSERT `{self.dataset}.location_partners` "
            "(partner_code, partner_name, machine_specification, created_at, created_by) "
            "VALUES (@partner_code, @partner_name, @machine_specification, @created_at, @created_by)"
        )
        self._run(
            query,
            [
                bigquery.ScalarQueryParameter("partner_code", "STRING", partner_code),
                bigquery.ScalarQueryParameter("partner_name", "STRING", partner_name),
                bigquery.ScalarQueryParameter("machine_specification", "STRING", machine_specification),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.now(timezone.utc)),
                bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
            ],
        ).result()

    def list_location_codes_paginated(
        self,
        page: int,
        page_size: int,
        q: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        # Return paginated location code rows ordered newest-first for predictable table navigation.
        where_clause = ""
        params: List[bigquery.ScalarQueryParameter] = []
        if q:
            # Apply substring filtering over the full location code text, matching the UI search behavior.
            where_clause = "WHERE CONTAINS_SUBSTR(location_id, @query)"
            params.append(bigquery.ScalarQueryParameter("query", "STRING", q))

        count_query = f"SELECT COUNT(1) AS total FROM `{self.dataset}.location_codes` {where_clause}"
        total_rows = list(self._run(count_query, params).result())
        total = int(total_rows[0]["total"]) if total_rows else 0

        # Include deterministic tie-breakers to keep pagination stable for equal timestamps.
        query = (
            f"SELECT location_id, set_code, weight_code, batch_variant_code, partner_code, production_date, created_at, created_by "
            f"FROM `{self.dataset}.location_codes` "
            f"{where_clause} "
            "ORDER BY created_at DESC, location_id DESC "
            "LIMIT @limit OFFSET @offset"
        )
        offset = max(page - 1, 0) * page_size
        data_params = [
            *params,
            bigquery.ScalarQueryParameter("limit", "INT64", page_size),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
        rows = self._run(query, data_params).result()
        return [dict(row) for row in rows], total

    def list_location_code_ids(self) -> List[str]:
        # Return active location IDs for dropdown options used when generating processing codes.
        query = f"SELECT DISTINCT location_id FROM `{self.dataset}.location_codes` ORDER BY location_id"
        rows = self._run(query, []).result()
        return [row["location_id"] for row in rows]

    def create_compounding_how(
        self,
        processing_code: str,
        location_code: str,
        process_code_suffix: str,
        failure_mode: str,
        machine_setup_url: Optional[str],
        processed_data_url: Optional[str],
        created_by: Optional[str],
    ) -> None:
        # Insert immutable core metadata with mutable link fields for later edits.
        query = (
            f"INSERT `{self.dataset}.compounding_how` "
            "(processing_code, location_code, process_code_suffix, failure_mode, machine_setup_url, "
            "processed_data_url, created_at, updated_at, created_by, updated_by, is_active) "
            "VALUES (@processing_code, @location_code, @process_code_suffix, @failure_mode, @machine_setup_url, "
            "@processed_data_url, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @created_by, @updated_by, TRUE)"
        )
        self._run(
            query,
            [
                bigquery.ScalarQueryParameter("processing_code", "STRING", processing_code),
                bigquery.ScalarQueryParameter("location_code", "STRING", location_code),
                bigquery.ScalarQueryParameter("process_code_suffix", "STRING", process_code_suffix),
                bigquery.ScalarQueryParameter("failure_mode", "STRING", failure_mode),
                bigquery.ScalarQueryParameter("machine_setup_url", "STRING", machine_setup_url),
                bigquery.ScalarQueryParameter("processed_data_url", "STRING", processed_data_url),
                bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
                bigquery.ScalarQueryParameter("updated_by", "STRING", created_by),
            ],
        ).result()

    def list_compounding_how(self) -> List[Dict[str, Any]]:
        # List active compounding records for the table below the form.
        query = (
            f"SELECT processing_code, location_code, process_code_suffix, failure_mode, machine_setup_url, "
            "processed_data_url, created_at, updated_at, created_by, updated_by "
            f"FROM `{self.dataset}.compounding_how` WHERE is_active = TRUE "
            "ORDER BY created_at DESC, processing_code DESC"
        )
        rows = self._run(query, []).result()
        return [dict(row) for row in rows]

    def update_compounding_how(
        self,
        processing_code: str,
        failure_mode: str,
        machine_setup_url: Optional[str],
        processed_data_url: Optional[str],
        updated_by: Optional[str],
    ) -> None:
        # Restrict updates to editable fields only, preserving immutable identifiers and timestamps.
        query = (
            f"UPDATE `{self.dataset}.compounding_how` "
            "SET failure_mode = @failure_mode, machine_setup_url = @machine_setup_url, "
            "processed_data_url = @processed_data_url, updated_at = CURRENT_TIMESTAMP(), updated_by = @updated_by "
            "WHERE processing_code = @processing_code AND is_active = TRUE"
        )
        self._run(
            query,
            [
                bigquery.ScalarQueryParameter("failure_mode", "STRING", failure_mode),
                bigquery.ScalarQueryParameter("machine_setup_url", "STRING", machine_setup_url),
                bigquery.ScalarQueryParameter("processed_data_url", "STRING", processed_data_url),
                bigquery.ScalarQueryParameter("updated_by", "STRING", updated_by),
                bigquery.ScalarQueryParameter("processing_code", "STRING", processing_code),
            ],
        ).result()

    def insert_location_code(
        self,
        set_code: str,
        weight_code: str,
        batch_variant_code: str,
        partner_code: str,
        production_date: str,
        location_id: str,
        created_by: Optional[str],
    ) -> None:
        # Store generated location IDs so batch traceability records can be audited later.
        query = (
            f"INSERT `{self.dataset}.location_codes` "
            "(set_code, weight_code, batch_variant_code, partner_code, production_date, location_id, created_at, created_by) "
            "VALUES (@set_code, @weight_code, @batch_variant_code, @partner_code, @production_date, @location_id, @created_at, @created_by)"
        )
        self._run(
            query,
            [
                bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                bigquery.ScalarQueryParameter("weight_code", "STRING", weight_code),
                bigquery.ScalarQueryParameter("batch_variant_code", "STRING", batch_variant_code),
                bigquery.ScalarQueryParameter("partner_code", "STRING", partner_code),
                bigquery.ScalarQueryParameter("production_date", "STRING", production_date),
                bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.now(timezone.utc)),
                bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
            ],
        ).result()
