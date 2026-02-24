from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from app.services.codegen_service import int_to_code


LOGGER = logging.getLogger(__name__)


@dataclass
class BigQueryService:
    project_id: str
    dataset_id: str
    bq_location: Optional[str] = None

    def __post_init__(self) -> None:
        # Create one shared BigQuery client for the service lifecycle to reuse pooled transport connections.
        self.client = bigquery.Client(project=self.project_id)

    def _resolve_query_location(self) -> Optional[str]:
        # Build an ordered list of location candidates so explicit constructor config wins over environment defaults.
        raw_candidates = [self.bq_location, os.getenv("BQ_LOCATION"), os.getenv("REGION")]
        # Strip candidate values and ignore blank entries so accidental empty env vars do not break query execution.
        candidates = [value.strip() for value in raw_candidates if value and value.strip()]
        # Choose the first non-empty location value, keeping None when no location is configured.
        location = candidates[0] if candidates else None
        # Force an EU-safe default when the dataset appears to be EU-scoped but no explicit location is configured.
        if location is None and (self.dataset_id.lower().endswith("_eu") or "_eu_" in self.dataset_id.lower() or self.dataset_id.lower().endswith("eu")):
            location = "europe-west2"
        return location

    @property
    def dataset(self) -> str:
        # Build the fully-qualified dataset reference used throughout all SQL statements.
        return f"{self.project_id}.{self.dataset_id}"

    def _run(self, query: str, params: Sequence[bigquery.ScalarQueryParameter]) -> bigquery.job.QueryJob:
        # Resolve the query location for each execution so runtime env updates are respected consistently.
        location = self._resolve_query_location()
        # Construct a query job config for typed parameters only; location must be passed to client.query itself.
        job_config = bigquery.QueryJobConfig(query_parameters=list(params))
        # Execute every query through a single helper path so all jobs target the same explicit BigQuery region.
        return self.client.query(query, job_config=job_config, location=location)

    def _render_sql(self, raw_sql: str) -> str:
        # Resolve template placeholders so infra SQL files can remain environment-agnostic in source control.
        return raw_sql.replace("PROJECT_ID", self.project_id).replace("DATASET_ID", self.dataset_id)

    def _run_sql_file(self, sql_file: Path) -> None:
        # Read and render one SQL file before splitting into statements for idempotent sequential execution.
        rendered = self._render_sql(sql_file.read_text(encoding="utf-8"))
        # Execute each non-empty statement separately so one file can contain multi-statement migrations.
        for statement in (part.strip() for part in rendered.split(";") if part.strip()):
            self._run(statement, []).result()

    def ensure_tables(self) -> None:
        # Apply schema + seed + views in deterministic order so startup repairs drift in empty or partially built datasets.
        ordered_files = [
            Path("infra/bigquery/ddl/001_create_tables.sql"),
            Path("infra/bigquery/ddl/002_seed_counters.sql"),
            Path("infra/bigquery/views/101_views.sql"),
        ]
        # Log effective migration context to make region/debug issues obvious in Cloud Run startup logs.
        LOGGER.info(
            "Ensuring BigQuery schema project=%s dataset=%s location=%s",
            self.project_id,
            self.dataset_id,
            self._resolve_query_location(),
        )
        # Execute the required baseline files first, then run remaining DDL files for additive backfill migrations.
        for sql_file in ordered_files:
            if sql_file.exists():
                self._run_sql_file(sql_file)

        # Include any additional DDL files not in the baseline list so later migrations are also applied at startup.
        baseline_names = {path.name for path in ordered_files}
        for sql_file in sorted(Path("infra/bigquery/ddl").glob("*.sql")):
            if sql_file.name in baseline_names:
                continue
            self._run_sql_file(sql_file)

        # Validate critical runtime columns immediately so schema drift fails fast with an actionable message.
        self.validate_required_schema()
        LOGGER.info("BigQuery startup migration completed")

    def run_startup_sql(self) -> None:
        # Keep the legacy startup method as a compatibility wrapper while delegating to ensure_tables.
        self.ensure_tables()

    def validate_required_schema(self) -> None:
        # Enumerate the minimum table->columns contract required by API/service query paths.
        required_columns = {
            "ingredients": {
                "sku", "category_code", "seq", "pack_size_value", "pack_size_unit", "trade_name_inci", "supplier", "spec_grade", "format", "created_at", "updated_at", "created_by", "updated_by", "is_active", "msds_object_path", "msds_filename", "msds_content_type", "msds_uploaded_at",
            },
            "ingredient_batches": {
                "sku", "ingredient_batch_code", "received_at", "notes", "quantity_value", "quantity_unit", "created_at", "updated_at", "created_by", "updated_by", "is_active", "spec_object_path", "spec_uploaded_at",
            },
            "location_partners": {"partner_code", "partner_name", "machine_specification", "created_at", "created_by"},
            "location_codes": {"set_code", "weight_code", "batch_variant_code", "partner_code", "production_date", "location_id", "created_at", "created_by"},
            "compounding_how": {"processing_code", "location_code", "process_code_suffix", "failure_mode", "machine_setup_url", "processed_data_url", "created_at", "updated_at", "created_by", "updated_by", "is_active"},
            "code_counters": {"counter_name", "scope", "next_value", "updated_at"},
            "pellet_bags": {"pellet_bag_id", "pellet_bag_code", "pellet_bag_code_tokens", "compounding_how_code", "product_type", "sequence_number", "bag_mass_kg", "remaining_mass_kg", "short_moisture_percent", "purpose", "reference_sample_taken", "qc_status", "long_moisture_status", "density_status", "injection_moulding_status", "film_forming_status", "injection_moulding_assignee_email", "film_forming_assignee_email", "notes", "customer", "created_at", "updated_at", "created_by", "updated_by", "is_active"},
            "pellet_bag_assignees": {"email", "is_active", "created_at", "created_by"},
            "conversion1_context": {"context_code", "pellet_bag_code", "partner_code", "machine_code", "date_yymmdd", "created_at", "created_by", "updated_at", "updated_by", "is_active"},
            "conversion1_how": {"how_code", "context_code", "process_code", "process_id", "notes", "failure_mode", "setup_link", "processed_data_link", "created_at", "created_by", "updated_at", "updated_by", "is_active"},
        }

        # Query INFORMATION_SCHEMA once and build a lookup map for compact validation logic.
        rows = self._run(
            (
                f"SELECT table_name, column_name FROM `{self.dataset}.INFORMATION_SCHEMA.COLUMNS` "
                "WHERE table_name IN UNNEST(@table_names)"
            ),
            [bigquery.ArrayQueryParameter("table_names", "STRING", list(required_columns.keys()))],
        ).result()

        table_columns: Dict[str, set[str]] = {}
        for row in rows:
            table_columns.setdefault(row["table_name"], set()).add(row["column_name"])

        # Accumulate all missing-table and missing-column issues so startup surfaces one complete actionable error.
        problems: List[str] = []
        for table_name, required in required_columns.items():
            existing = table_columns.get(table_name)
            if not existing:
                problems.append(f"missing table `{table_name}`")
                continue
            missing_columns = sorted(required - existing)
            if missing_columns:
                problems.append(f"table `{table_name}` missing columns: {', '.join(missing_columns)}")

        if problems:
            message = "BigQuery schema validation failed: " + "; ".join(problems)
            LOGGER.error(message)
            raise RuntimeError(message)

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
        # Preserve compatibility for legacy callers that looked up a sequence without category scope.
        query = f"SELECT * FROM `{self.dataset}.ingredients` WHERE seq = @seq LIMIT 1"
        rows = self._run(query, [bigquery.ScalarQueryParameter("seq", "INT64", seq)]).result()
        for row in rows:
            return dict(row)
        return None

    def find_ingredient_by_category_and_seq(self, category_code: int, seq: int) -> Optional[Dict[str, Any]]:
        # Enforce uniqueness within category+sequence, matching the SKU structure <category>_<seq>_<pack_size>.
        query = (
            f"SELECT * FROM `{self.dataset}.ingredients` "
            "WHERE category_code = @category_code AND seq = @seq LIMIT 1"
        )
        rows = self._run(
            query,
            [
                bigquery.ScalarQueryParameter("category_code", "INT64", category_code),
                bigquery.ScalarQueryParameter("seq", "INT64", seq),
            ],
        ).result()
        for row in rows:
            return dict(row)
        return None

    def set_counter_at_least(self, counter_name: str, scope: str, minimum_next_value: int) -> None:
        # Raise a counter floor without consuming a value so imported IDs are respected by later generated codes.
        query = (
            f"MERGE `{self.dataset}.code_counters` T "
            "USING (SELECT @counter_name AS counter_name, @scope AS scope, @minimum_next_value AS minimum_next_value) S "
            "ON T.counter_name = S.counter_name AND T.scope = S.scope "
            "WHEN MATCHED THEN "
            "  UPDATE SET next_value = IF(T.next_value < S.minimum_next_value, S.minimum_next_value, T.next_value), "
            "             updated_at = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN "
            "  INSERT (counter_name, scope, next_value, updated_at) "
            "  VALUES (S.counter_name, S.scope, S.minimum_next_value, CURRENT_TIMESTAMP())"
        )
        self._run(
            query,
            [
                bigquery.ScalarQueryParameter("counter_name", "STRING", counter_name),
                bigquery.ScalarQueryParameter("scope", "STRING", scope),
                bigquery.ScalarQueryParameter("minimum_next_value", "INT64", minimum_next_value),
            ],
        ).result()

    def list_ingredients(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Build optional WHERE predicates from supplied filters while keeping query parameters fully typed.
        where = []
        # Collect query parameters centrally so all filters remain SQL-injection safe.
        params: List[bigquery.ScalarQueryParameter] = []
        if "q" in filters:
            # Apply case-insensitive search by comparing lower-cased columns to a lower-cased search token.
            where.append(
                "(LOWER(sku) LIKE @q OR LOWER(trade_name_inci) LIKE @q OR LOWER(supplier) LIKE @q)"
            )
            # Normalise the search term to lower case so user input casing never affects matches.
            params.append(bigquery.ScalarQueryParameter("q", "STRING", f"%{str(filters['q']).lower()}%"))
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

    def get_mixing_partner_machine_options(self) -> List[Dict[str, Any]]:
        # Reuse existing location-partner records as machine+partner options for conversion workflows.
        query = (
            f"SELECT partner_code, partner_name, machine_specification AS machine_code "
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

    def formulation_exists(self, set_code: str, weight_code: str, batch_variant_code: str) -> bool:
        # Verify requested location-code formulation components reference an existing formulation record.
        query = (
            f"SELECT 1 FROM `{self.dataset}.v_formulations_flat` "
            "WHERE set_code = @set_code AND weight_code = @weight_code AND batch_variant_code = @batch_variant_code "
            "LIMIT 1"
        )
        rows = list(
            self._run(
                query,
                [
                    bigquery.ScalarQueryParameter("set_code", "STRING", set_code),
                    bigquery.ScalarQueryParameter("weight_code", "STRING", weight_code),
                    bigquery.ScalarQueryParameter("batch_variant_code", "STRING", batch_variant_code),
                ],
            ).result()
        )
        return bool(rows)

    def list_distinct_formulation_codes(self) -> List[Dict[str, str]]:
        # Provide unique formulation code parts for dropdown/manual assist in the location-code create form.
        primary_query = (
            f"SELECT DISTINCT set_code, weight_code, batch_variant_code FROM `{self.dataset}.v_formulations_flat` "
            "ORDER BY set_code, weight_code, batch_variant_code"
        )
        try:
            rows = self._run(primary_query, []).result()
            return [dict(row) for row in rows]
        except NotFound:
            # Fall back to the base batch-variant table when the flat view is temporarily missing in a region.
            fallback_query = (
                f"SELECT DISTINCT set_code, weight_code, batch_variant_code FROM `{self.dataset}.batch_variants` "
                "ORDER BY set_code, weight_code, batch_variant_code"
            )
            rows = self._run(fallback_query, []).result()
            return [dict(row) for row in rows]

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

    def create_or_get_conversion1_context(
        self,
        pellet_code: str,
        partner_code: str,
        machine_code: str,
        date_yymmdd: str,
        user_email: Optional[str],
    ) -> Dict[str, Any]:
        # Build deterministic context code from pellet code + partner code + YYMMDD token.
        context_code = f"{pellet_code} {partner_code} {date_yymmdd}".strip()
        # Return existing active row when the same deterministic code has already been persisted.
        existing = self.get_conversion1_context(context_code)
        if existing:
            return existing
        # Insert a new conversion context row for first-time deterministic code creation.
        query = (
            f"INSERT `{self.dataset}.conversion1_context` "
            "(context_code, pellet_bag_code, partner_code, machine_code, date_yymmdd, created_at, created_by, updated_at, updated_by, is_active) "
            "VALUES (@context_code, @pellet_bag_code, @partner_code, @machine_code, @date_yymmdd, CURRENT_TIMESTAMP(), @created_by, CURRENT_TIMESTAMP(), @updated_by, TRUE)"
        )
        self._run(
            query,
            [
                bigquery.ScalarQueryParameter("context_code", "STRING", context_code),
                bigquery.ScalarQueryParameter("pellet_bag_code", "STRING", pellet_code),
                bigquery.ScalarQueryParameter("partner_code", "STRING", partner_code),
                bigquery.ScalarQueryParameter("machine_code", "STRING", machine_code),
                bigquery.ScalarQueryParameter("date_yymmdd", "STRING", date_yymmdd),
                bigquery.ScalarQueryParameter("created_by", "STRING", user_email),
                bigquery.ScalarQueryParameter("updated_by", "STRING", user_email),
            ],
        ).result()
        return self.get_conversion1_context(context_code) or {"context_code": context_code}

    def get_conversion1_context(self, context_code: str) -> Optional[Dict[str, Any]]:
        # Retrieve one active conversion context row to support conversion-how form validation.
        query = (
            f"SELECT context_code, pellet_bag_code, partner_code, machine_code, date_yymmdd, created_at, created_by, updated_at, updated_by "
            f"FROM `{self.dataset}.conversion1_context` WHERE context_code = @context_code AND is_active = TRUE LIMIT 1"
        )
        rows = list(self._run(query, [bigquery.ScalarQueryParameter("context_code", "STRING", context_code)]).result())
        return dict(rows[0]) if rows else None

    def create_or_update_conversion1_how(
        self,
        context_code: str,
        notes: Optional[str],
        failure_mode: Optional[str],
        setup_link: Optional[str],
        processed_data_link: Optional[str],
        user_email: Optional[str],
    ) -> Dict[str, Any]:
        # Mint the next AB-style process code from a dedicated conversion counter.
        process_code_value = self.allocate_counter("conversion1_process_code", "", start_value=1)
        process_code = int_to_code(process_code_value)
        # Compute how/process identifiers from deterministic context code + generated process code.
        how_code = f"{context_code} {process_code}".strip()
        process_id = how_code
        # Persist one immutable conversion-how record with optional links and metadata.
        query = (
            f"INSERT `{self.dataset}.conversion1_how` "
            "(how_code, context_code, process_code, process_id, notes, failure_mode, setup_link, processed_data_link, created_at, created_by, updated_at, updated_by, is_active) "
            "VALUES (@how_code, @context_code, @process_code, @process_id, @notes, @failure_mode, @setup_link, @processed_data_link, CURRENT_TIMESTAMP(), @created_by, CURRENT_TIMESTAMP(), @updated_by, TRUE)"
        )
        self._run(
            query,
            [
                bigquery.ScalarQueryParameter("how_code", "STRING", how_code),
                bigquery.ScalarQueryParameter("context_code", "STRING", context_code),
                bigquery.ScalarQueryParameter("process_code", "STRING", process_code),
                bigquery.ScalarQueryParameter("process_id", "STRING", process_id),
                bigquery.ScalarQueryParameter("notes", "STRING", notes),
                bigquery.ScalarQueryParameter("failure_mode", "STRING", failure_mode),
                bigquery.ScalarQueryParameter("setup_link", "STRING", setup_link),
                bigquery.ScalarQueryParameter("processed_data_link", "STRING", processed_data_link),
                bigquery.ScalarQueryParameter("created_by", "STRING", user_email),
                bigquery.ScalarQueryParameter("updated_by", "STRING", user_email),
            ],
        ).result()
        return {
            "how_code": how_code,
            "context_code": context_code,
            "process_code": process_code,
            "process_id": process_id,
            "notes": notes,
            "failure_mode": failure_mode,
            "setup_link": setup_link,
            "processed_data_link": processed_data_link,
        }

    def list_conversion1_how(
        self,
        context_code: Optional[str],
        process_id: Optional[str],
        failure_mode: Optional[str],
        search: Optional[str],
        page: int,
        page_size: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        # Build dynamic predicates for conversion-how filtering while keeping typed parameters safe.
        where: List[str] = ["is_active = TRUE"]
        params: List[bigquery.ScalarQueryParameter] = []
        if context_code:
            where.append("context_code = @context_code")
            params.append(bigquery.ScalarQueryParameter("context_code", "STRING", context_code))
        if process_id:
            where.append("process_id = @process_id")
            params.append(bigquery.ScalarQueryParameter("process_id", "STRING", process_id))
        if failure_mode:
            where.append("failure_mode = @failure_mode")
            params.append(bigquery.ScalarQueryParameter("failure_mode", "STRING", failure_mode))
        if search:
            where.append("(CONTAINS_SUBSTR(how_code, @search) OR CONTAINS_SUBSTR(notes, @search))")
            params.append(bigquery.ScalarQueryParameter("search", "STRING", search))
        where_clause = "WHERE " + " AND ".join(where)
        # Query total rows separately so UI pagination controls can render correctly.
        count_query = f"SELECT COUNT(1) AS total FROM `{self.dataset}.conversion1_how` {where_clause}"
        total_rows = list(self._run(count_query, params).result())
        total = int(total_rows[0]["total"]) if total_rows else 0
        # Return one ordered page of conversion-how records with stable tie-breaker sorting.
        query = (
            f"SELECT how_code, context_code, process_code, process_id, notes, failure_mode, setup_link, processed_data_link, created_at, created_by, updated_at, updated_by "
            f"FROM `{self.dataset}.conversion1_how` {where_clause} "
            "ORDER BY created_at DESC, how_code DESC LIMIT @limit OFFSET @offset"
        )
        offset = max(page - 1, 0) * page_size
        data_params = [*params, bigquery.ScalarQueryParameter("limit", "INT64", page_size), bigquery.ScalarQueryParameter("offset", "INT64", offset)]
        rows = self._run(query, data_params).result()
        return [dict(row) for row in rows], total

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

    def get_next_compounding_process_suffix(self, start_value: int = 1) -> Optional[str]:
        # Compute the next suffix from persisted submissions only, ignoring unsaved UI generations.
        query = (
            f"SELECT process_code_suffix FROM `{self.dataset}.compounding_how` "
            "WHERE is_active = TRUE ORDER BY process_code_suffix DESC LIMIT 1"
        )
        rows = list(self._run(query, []).result())
        if not rows:
            return None
        return rows[0].get("process_code_suffix")

    def processing_code_exists(self, processing_code: str) -> bool:
        # Allow API-layer conflict checks before inserting immutable processing codes.
        query = (
            f"SELECT 1 FROM `{self.dataset}.compounding_how` "
            "WHERE processing_code = @processing_code AND is_active = TRUE LIMIT 1"
        )
        rows = list(
            self._run(query, [bigquery.ScalarQueryParameter("processing_code", "STRING", processing_code)]).result()
        )
        return bool(rows)

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

    def list_compounding_how_codes(self) -> List[str]:
        # Return only active processing codes so forms can enforce valid compounding references.
        query = (
            f"SELECT processing_code FROM `{self.dataset}.compounding_how` "
            "WHERE is_active = TRUE ORDER BY processing_code DESC"
        )
        rows = self._run(query, []).result()
        return [str(row["processing_code"]) for row in rows if row.get("processing_code")]

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

    def allocate_counter_range(self, counter_name: str, scope: str, start_value: int, count: int) -> int:
        # Allocate an atomic contiguous range of counter values and return the starting value.
        if count < 1:
            raise ValueError("count must be >= 1")
        for _ in range(20):
            current_value = self.allocate_counter(counter_name=counter_name, scope=scope, start_value=start_value)
            if count == 1:
                return current_value
            update_query = (
                f"UPDATE `{self.dataset}.code_counters` "
                "SET next_value = @next_value, updated_at = CURRENT_TIMESTAMP() "
                "WHERE counter_name = @counter_name AND scope = @scope AND next_value = @expected_next_value"
            )
            update_job = self._run(
                update_query,
                [
                    bigquery.ScalarQueryParameter("next_value", "INT64", current_value + count),
                    bigquery.ScalarQueryParameter("counter_name", "STRING", counter_name),
                    bigquery.ScalarQueryParameter("scope", "STRING", scope),
                    bigquery.ScalarQueryParameter("expected_next_value", "INT64", current_value + 1),
                ],
            )
            update_job.result()
            if update_job.num_dml_affected_rows == 1:
                return current_value
        raise RuntimeError("Failed to allocate counter range after retries")

    def list_pellet_bag_assignees(self, default_emails: Optional[List[str]] = None) -> List[str]:
        # Return active assignee emails from table, seeding defaults once when table is empty.
        query = f"SELECT email FROM `{self.dataset}.pellet_bag_assignees` WHERE is_active = TRUE ORDER BY email"
        rows = [dict(row) for row in self._run(query, []).result()]
        if rows:
            return [row["email"] for row in rows]
        seed_values = default_emails or []
        if seed_values:
            for email in seed_values:
                self._run(
                    f"INSERT `{self.dataset}.pellet_bag_assignees` (email, is_active, created_at, created_by) "
                    "VALUES (@email, TRUE, CURRENT_TIMESTAMP(), @created_by)",
                    [
                        bigquery.ScalarQueryParameter("email", "STRING", email),
                        bigquery.ScalarQueryParameter("created_by", "STRING", "system"),
                    ],
                ).result()
        return sorted(seed_values)

    def create_pellet_bags(
        self,
        compounding_how_code: str,
        product_type: str,
        bag_mass_kg: float,
        number_of_bags: int,
        optional_fields: Dict[str, Any],
        created_by: Optional[str],
    ) -> List[Dict[str, Any]]:
        # Use one shared counter scope so PR/PF/PI bag numbering increments globally across all product types.
        scope = "pellet_bag:global"
        start_sequence = self.allocate_counter_range("pellet_bag_sequence", scope, start_value=0, count=number_of_bags)
        compounding_tokens = compounding_how_code.split()
        created_items: List[Dict[str, Any]] = []

        for offset in range(number_of_bags):
            sequence_number = start_sequence + offset
            sequence_token = f"{sequence_number:04d}"
            pellet_tokens = [*compounding_tokens, product_type, sequence_token]
            pellet_bag_code = " ".join(pellet_tokens)
            pellet_bag_id = str(uuid4())
            remaining_mass = optional_fields.get("remaining_mass_kg")
            if remaining_mass is None:
                remaining_mass = bag_mass_kg

            self._run(
                f"INSERT `{self.dataset}.pellet_bags` "
                "(pellet_bag_id, pellet_bag_code, pellet_bag_code_tokens, compounding_how_code, product_type, sequence_number, "
                "bag_mass_kg, remaining_mass_kg, short_moisture_percent, purpose, reference_sample_taken, qc_status, "
                "long_moisture_status, density_status, injection_moulding_status, film_forming_status, "
                "injection_moulding_assignee_email, film_forming_assignee_email, notes, customer, "
                "created_at, updated_at, created_by, updated_by, is_active) "
                "VALUES (@pellet_bag_id, @pellet_bag_code, @pellet_bag_code_tokens, @compounding_how_code, @product_type, @sequence_number, "
                "@bag_mass_kg, @remaining_mass_kg, @short_moisture_percent, @purpose, @reference_sample_taken, @qc_status, "
                "@long_moisture_status, @density_status, @injection_moulding_status, @film_forming_status, "
                "@injection_moulding_assignee_email, @film_forming_assignee_email, @notes, @customer, "
                "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @created_by, @updated_by, TRUE)",
                [
                    bigquery.ScalarQueryParameter("pellet_bag_id", "STRING", pellet_bag_id),
                    bigquery.ScalarQueryParameter("pellet_bag_code", "STRING", pellet_bag_code),
                    bigquery.ArrayQueryParameter("pellet_bag_code_tokens", "STRING", pellet_tokens),
                    bigquery.ScalarQueryParameter("compounding_how_code", "STRING", compounding_how_code),
                    bigquery.ScalarQueryParameter("product_type", "STRING", product_type),
                    bigquery.ScalarQueryParameter("sequence_number", "INT64", sequence_number),
                    bigquery.ScalarQueryParameter("bag_mass_kg", "FLOAT64", bag_mass_kg),
                    bigquery.ScalarQueryParameter("remaining_mass_kg", "FLOAT64", remaining_mass),
                    bigquery.ScalarQueryParameter("short_moisture_percent", "FLOAT64", optional_fields.get("short_moisture_percent")),
                    bigquery.ScalarQueryParameter("purpose", "STRING", optional_fields.get("purpose")),
                    bigquery.ScalarQueryParameter("reference_sample_taken", "STRING", optional_fields.get("reference_sample_taken")),
                    bigquery.ScalarQueryParameter("qc_status", "STRING", optional_fields.get("qc_status")),
                    bigquery.ScalarQueryParameter("long_moisture_status", "STRING", optional_fields.get("long_moisture_status")),
                    bigquery.ScalarQueryParameter("density_status", "STRING", optional_fields.get("density_status")),
                    bigquery.ScalarQueryParameter("injection_moulding_status", "STRING", optional_fields.get("injection_moulding_status")),
                    bigquery.ScalarQueryParameter("film_forming_status", "STRING", optional_fields.get("film_forming_status")),
                    bigquery.ScalarQueryParameter("injection_moulding_assignee_email", "STRING", optional_fields.get("injection_moulding_assignee_email")),
                    bigquery.ScalarQueryParameter("film_forming_assignee_email", "STRING", optional_fields.get("film_forming_assignee_email")),
                    bigquery.ScalarQueryParameter("notes", "STRING", optional_fields.get("notes")),
                    bigquery.ScalarQueryParameter("customer", "STRING", optional_fields.get("customer")),
                    bigquery.ScalarQueryParameter("created_by", "STRING", created_by),
                    bigquery.ScalarQueryParameter("updated_by", "STRING", created_by),
                ],
            ).result()

            created_items.append({
                "pellet_bag_id": pellet_bag_id,
                "pellet_bag_code": pellet_bag_code,
                "pellet_bag_code_tokens": pellet_tokens,
                "compounding_how_code": compounding_how_code,
                "product_type": product_type,
                "sequence_number": sequence_number,
                "bag_mass_kg": bag_mass_kg,
                "remaining_mass_kg": remaining_mass,
                "short_moisture_percent": optional_fields.get("short_moisture_percent"),
                "purpose": optional_fields.get("purpose"),
                "reference_sample_taken": optional_fields.get("reference_sample_taken"),
                "qc_status": optional_fields.get("qc_status"),
                "long_moisture_status": optional_fields.get("long_moisture_status"),
                "density_status": optional_fields.get("density_status"),
                "injection_moulding_status": optional_fields.get("injection_moulding_status"),
                "film_forming_status": optional_fields.get("film_forming_status"),
                "injection_moulding_assignee_email": optional_fields.get("injection_moulding_assignee_email"),
                "film_forming_assignee_email": optional_fields.get("film_forming_assignee_email"),
                "notes": optional_fields.get("notes"),
                "customer": optional_fields.get("customer"),
                "created_by": created_by,
            })
        return created_items


    def get_sku_summary(self, sku: str) -> Dict[str, List[Dict[str, Any]] | Optional[Dict[str, Any]]]:
        # Fetch ingredient record plus related formulation and pellet bag links for the SKU detail page.
        ingredient = self.get_ingredient(sku)
        formulations_query = (
            f"SELECT set_code, weight_code, batch_variant_code, base_code, created_at "
            f"FROM `{self.dataset}.v_formulations_flat` "
            "WHERE EXISTS (SELECT 1 FROM UNNEST(sku_list) AS listed_sku WHERE listed_sku = @sku) "
            "ORDER BY created_at DESC"
        )
        formulations = [
            dict(row)
            for row in self._run(formulations_query, [bigquery.ScalarQueryParameter("sku", "STRING", sku)]).result()
        ]
        pellet_query = (
            f"SELECT DISTINCT p.pellet_bag_id, p.pellet_bag_code, p.compounding_how_code, p.updated_at, p.created_at "
            f"FROM `{self.dataset}.pellet_bags` p "
            f"JOIN `{self.dataset}.compounding_how` c ON c.processing_code = p.compounding_how_code "
            f"JOIN `{self.dataset}.v_formulations_flat` f "
            "ON f.set_code = SPLIT(c.location_code, ' ')[SAFE_OFFSET(0)] "
            "AND f.weight_code = SPLIT(c.location_code, ' ')[SAFE_OFFSET(1)] "
            "AND f.batch_variant_code = SPLIT(c.location_code, ' ')[SAFE_OFFSET(2)] "
            "WHERE p.is_active = TRUE "
            "AND EXISTS (SELECT 1 FROM UNNEST(f.sku_list) AS listed_sku WHERE LOWER(listed_sku) = LOWER(@sku)) "
            "ORDER BY updated_at DESC, created_at DESC"
        )
        pellet_bags = [
            dict(row)
            for row in self._run(pellet_query, [bigquery.ScalarQueryParameter("sku", "STRING", sku)]).result()
        ]
        return {"ingredient": ingredient, "formulations": formulations, "pellet_bags": pellet_bags}

    def get_pellet_bag_detail(self, pellet_bag_code: str) -> Optional[Dict[str, Any]]:
        # Load one pellet bag row and enrich it with compounding + location partner context for the detail page.
        pellet_query = (
            f"SELECT p.*, "
            "c.location_code, "
            "c.failure_mode, "
            "c.machine_setup_url, "
            "c.processed_data_url, "
            "lp.partner_name AS compounding_partner_name, "
            "COALESCE(lp.machine_specification, lc.partner_code) AS machine "
            f"FROM `{self.dataset}.pellet_bags` p "
            f"LEFT JOIN `{self.dataset}.compounding_how` c ON c.processing_code = p.compounding_how_code AND c.is_active = TRUE "
            f"LEFT JOIN `{self.dataset}.location_codes` lc ON lc.location_id = c.location_code "
            f"LEFT JOIN `{self.dataset}.location_partners` lp ON lp.partner_code = lc.partner_code "
            "WHERE p.pellet_bag_code = @pellet_bag_code AND p.is_active = TRUE LIMIT 1"
        )
        rows = list(self._run(pellet_query, [bigquery.ScalarQueryParameter("pellet_bag_code", "STRING", pellet_bag_code)]).result())
        if not rows:
            return None
        pellet = dict(rows[0])
        compounding = None
        if pellet.get("compounding_how_code"):
            comp_query = (
                f"SELECT * FROM `{self.dataset}.compounding_how` "
                "WHERE processing_code = @processing_code AND is_active = TRUE LIMIT 1"
            )
            comp_rows = list(self._run(comp_query, [bigquery.ScalarQueryParameter("processing_code", "STRING", pellet["compounding_how_code"])]).result())
            if comp_rows:
                compounding = dict(comp_rows[0])
        # Attach related formulation rows by decoding set/weight/batch tokens from the compounding location code.
        formulations = self.list_formulations_for_pellet_bag(pellet_bag_code)
        return {"pellet_bag": pellet, "compounding_how": compounding, "formulations": formulations}

    def list_formulations_for_pellet_bag(self, pellet_bag_code: str) -> List[Dict[str, Any]]:
        # Resolve formulation rows connected to a pellet bag via compounding_how.location_code token mapping.
        query = (
            f"SELECT f.* "
            f"FROM `{self.dataset}.pellet_bags` p "
            f"JOIN `{self.dataset}.compounding_how` c ON c.processing_code = p.compounding_how_code AND c.is_active = TRUE "
            f"JOIN `{self.dataset}.v_formulations_flat` f "
            "ON f.set_code = SPLIT(c.location_code, ' ')[SAFE_OFFSET(0)] "
            "AND f.weight_code = SPLIT(c.location_code, ' ')[SAFE_OFFSET(1)] "
            "AND f.batch_variant_code = SPLIT(c.location_code, ' ')[SAFE_OFFSET(2)] "
            "WHERE p.pellet_bag_code = @pellet_bag_code AND p.is_active = TRUE "
            "ORDER BY f.created_at DESC"
        )
        params = [bigquery.ScalarQueryParameter("pellet_bag_code", "STRING", pellet_bag_code)]
        return [dict(row) for row in self._run(query, params).result()]

    def list_pellet_bags_with_meaningful_status(self, status_column: str, limit: int = 25) -> List[Dict[str, Any]]:
        # Restrict status filters to known columns to avoid unsafe dynamic SQL.
        allowed_columns = {
            "long_moisture_status",
            "density_status",
            "injection_moulding_status",
            "film_forming_status",
            "qc_status",
        }
        if status_column not in allowed_columns:
            raise ValueError("Invalid status column")
        # Query rows where the chosen status is meaningful and not in excluded sentinel values.
        # Map each status stream to its dedicated assignee field; QC statuses fall back to creator ownership.
        assigned_expression = "created_by"
        if status_column == "injection_moulding_status":
            assigned_expression = "COALESCE(injection_moulding_assignee_email, created_by)"
        elif status_column == "film_forming_status":
            assigned_expression = "COALESCE(film_forming_assignee_email, created_by)"
        query = (
            f"SELECT pellet_bag_id, pellet_bag_code, {status_column} AS status_value, {assigned_expression} AS assigned_to, updated_at, created_at "
            f"FROM `{self.dataset}.pellet_bags` "
            f"WHERE is_active = TRUE AND {status_column} IS NOT NULL "
            f"AND TRIM({status_column}) != '' "
            f"AND LOWER(TRIM({status_column})) NOT IN ('not requested', 'not received') "
            "ORDER BY COALESCE(updated_at, created_at) DESC LIMIT @limit"
        )
        return [dict(row) for row in self._run(query, [bigquery.ScalarQueryParameter("limit", "INT64", limit)]).result()]

    def list_pellet_bags(self) -> List[Dict[str, Any]]:
        # Return active pellet bag records newest-first for the management table.
        query = (
            f"SELECT pellet_bag_id, pellet_bag_code, pellet_bag_code_tokens, compounding_how_code, product_type, sequence_number, "
            "bag_mass_kg, remaining_mass_kg, short_moisture_percent, purpose, reference_sample_taken, qc_status, "
            "long_moisture_status, density_status, injection_moulding_status, film_forming_status, "
            "injection_moulding_assignee_email, film_forming_assignee_email, notes, customer, created_at, updated_at, created_by, updated_by "
            f"FROM `{self.dataset}.pellet_bags` WHERE is_active = TRUE ORDER BY created_at DESC, sequence_number DESC"
        )
        return [dict(row) for row in self._run(query, []).result()]


    def get_dashboard_stats(self) -> Dict[str, Any]:
        # Fetch dashboard KPI values in one query so all cards reflect a consistent snapshot.
        query = (
            "SELECT "
            f"(SELECT COUNT(1) FROM `{self.dataset}.ingredients` WHERE is_active = TRUE) AS sku_count, "
            f"(SELECT COUNT(1) FROM `{self.dataset}.pellet_bags` WHERE is_active = TRUE) AS active_pellet_bags, "
            f"(SELECT COALESCE(SUM(bag_mass_kg), 0) FROM `{self.dataset}.pellet_bags` WHERE is_active = TRUE) AS total_pellets_produced_kg"
        )
        rows = list(self._run(query, []).result())
        if not rows:
            return {"sku_count": 0, "active_pellet_bags": 0, "total_pellets_produced_kg": 0.0}
        row = dict(rows[0])
        return {
            "sku_count": int(row.get("sku_count") or 0),
            "active_pellet_bags": int(row.get("active_pellet_bags") or 0),
            "total_pellets_produced_kg": float(row.get("total_pellets_produced_kg") or 0),
        }

    def update_pellet_bag(self, pellet_bag_id: str, updated_by: Optional[str], optional_fields: Dict[str, Any]) -> bool:
        # Update only editable optional fields while preserving immutable identifiers and creation metadata.
        query = (
            f"UPDATE `{self.dataset}.pellet_bags` SET "
            "remaining_mass_kg = COALESCE(@remaining_mass_kg, remaining_mass_kg), "
            "short_moisture_percent = COALESCE(@short_moisture_percent, short_moisture_percent), "
            "purpose = COALESCE(@purpose, purpose), "
            "reference_sample_taken = COALESCE(@reference_sample_taken, reference_sample_taken), "
            "qc_status = COALESCE(@qc_status, qc_status), "
            "long_moisture_status = COALESCE(@long_moisture_status, long_moisture_status), "
            "density_status = COALESCE(@density_status, density_status), "
            "injection_moulding_status = COALESCE(@injection_moulding_status, injection_moulding_status), "
            "film_forming_status = COALESCE(@film_forming_status, film_forming_status), "
            "injection_moulding_assignee_email = COALESCE(@injection_moulding_assignee_email, injection_moulding_assignee_email), "
            "film_forming_assignee_email = COALESCE(@film_forming_assignee_email, film_forming_assignee_email), "
            "notes = COALESCE(@notes, notes), "
            "customer = COALESCE(@customer, customer), "
            "updated_at = CURRENT_TIMESTAMP(), updated_by = @updated_by "
            "WHERE pellet_bag_id = @pellet_bag_id AND is_active = TRUE"
        )
        job = self._run(
            query,
            [
                bigquery.ScalarQueryParameter("remaining_mass_kg", "FLOAT64", optional_fields.get("remaining_mass_kg")),
                bigquery.ScalarQueryParameter("short_moisture_percent", "FLOAT64", optional_fields.get("short_moisture_percent")),
                bigquery.ScalarQueryParameter("purpose", "STRING", optional_fields.get("purpose")),
                bigquery.ScalarQueryParameter("reference_sample_taken", "STRING", optional_fields.get("reference_sample_taken")),
                bigquery.ScalarQueryParameter("qc_status", "STRING", optional_fields.get("qc_status")),
                bigquery.ScalarQueryParameter("long_moisture_status", "STRING", optional_fields.get("long_moisture_status")),
                bigquery.ScalarQueryParameter("density_status", "STRING", optional_fields.get("density_status")),
                bigquery.ScalarQueryParameter("injection_moulding_status", "STRING", optional_fields.get("injection_moulding_status")),
                bigquery.ScalarQueryParameter("film_forming_status", "STRING", optional_fields.get("film_forming_status")),
                bigquery.ScalarQueryParameter("injection_moulding_assignee_email", "STRING", optional_fields.get("injection_moulding_assignee_email")),
                bigquery.ScalarQueryParameter("film_forming_assignee_email", "STRING", optional_fields.get("film_forming_assignee_email")),
                bigquery.ScalarQueryParameter("notes", "STRING", optional_fields.get("notes")),
                bigquery.ScalarQueryParameter("customer", "STRING", optional_fields.get("customer")),
                bigquery.ScalarQueryParameter("updated_by", "STRING", updated_by),
                bigquery.ScalarQueryParameter("pellet_bag_id", "STRING", pellet_bag_id),
            ],
        )
        job.result()
        return bool(job.num_dml_affected_rows)

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
