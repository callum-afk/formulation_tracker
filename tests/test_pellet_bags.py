from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from fastapi import HTTPException

from app.api.pellet_bags_api import INJECTION_FILM_STATUS_OPTIONS, get_allowed_status_options, normalize_status_value, _validate_optional_payload
from app.models import PelletBagCreate
from app.services.bigquery_service import BigQueryService


class PelletBagTests(unittest.TestCase):
    def test_pellet_bag_create_normalizes_compounding_code_and_product_type(self) -> None:
        # Confirm creation payload normalization keeps compounding tokens and uppercases product type.
        payload = PelletBagCreate(compounding_how_code=" AB   AB  AC ", product_type="pr", bag_mass_kg=10)

        self.assertEqual(payload.compounding_how_code, "AB AB AC")
        self.assertEqual(payload.product_type, "PR")

    def test_injection_and_film_status_support_failed(self) -> None:
        # Ensure Failed appears for injection/film dropdowns per requirements.
        self.assertIn("Failed", INJECTION_FILM_STATUS_OPTIONS)

    def test_validation_rejects_invalid_status(self) -> None:
        # Reject unsupported status values for constrained optional dropdown fields.
        with self.assertRaises(HTTPException):
            _validate_optional_payload({"long_moisture_status": "Failed"})


    def test_create_pellet_bags_uses_global_sequence_scope(self) -> None:
        # Verify PR/PF/PI bag numbering allocates from one shared global counter scope.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"
        service.allocate_counter_range = MagicMock(return_value=12)

        # Stub insert execution to avoid external BigQuery calls while preserving method flow.
        fake_job = MagicMock()
        fake_job.result.return_value = None
        service._run = MagicMock(return_value=fake_job)

        created = service.create_pellet_bags(
            compounding_how_code="AC AB AB AM 260205 AC",
            product_type="PF",
            bag_mass_kg=25.0,
            number_of_bags=1,
            optional_fields={},
            created_by="tester@notpla.com",
        )

        service.allocate_counter_range.assert_called_once_with(
            "pellet_bag_sequence",
            "pellet_bag:global",
            start_value=0,
            count=1,
        )
        self.assertEqual(created[0]["pellet_bag_code"], "AC AB AB AM 260205 AC PF 0012")

    def test_list_pellet_bags_with_meaningful_status_selects_assignee_column(self) -> None:
        # Ensure the dashboard status query exposes assigned_to with the right assignee fallback per stream.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"

        # Capture SQL text and return no rows so we can assert query composition only.
        captured = {}

        class _FakeResult:
            def result(self):
                return []

        def fake_run(query, params):
            captured["query"] = query
            return _FakeResult()

        service._run = fake_run

        service.list_pellet_bags_with_meaningful_status("injection_moulding_status")

        self.assertIn("AS assigned_to", captured["query"])
        self.assertIn("COALESCE(injection_moulding_assignee_email, created_by)", captured["query"])

    def test_get_pellet_bag_detail_includes_formulations_payload(self) -> None:
        # Ensure pellet detail response now carries formulations for the shared formulation table component.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"

        # Return one pellet row and one compounding row while stubbing formulation lookup separately.
        pellet_row = [{"pellet_bag_code": "AB", "compounding_how_code": "CODE", "is_active": True}]
        compounding_row = [{"processing_code": "CODE", "is_active": True}]

        class _FakeResult:
            def __init__(self, rows):
                self._rows = rows

            def result(self):
                return self._rows

        call_count = {"index": 0}

        def fake_run(query, params):
            call_count["index"] += 1
            if call_count["index"] == 1:
                return _FakeResult(pellet_row)
            return _FakeResult(compounding_row)

        service._run = fake_run
        service.list_formulations_for_pellet_bag = MagicMock(return_value=[{"set_code": "AB"}])

        detail = service.get_pellet_bag_detail("AB")

        self.assertEqual(detail["formulations"], [{"set_code": "AB"}])

    def test_list_compounding_how_codes_returns_only_codes(self) -> None:
        # Ensure compounding dropdown metadata uses processing codes only and keeps ordering from query results.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"

        class _FakeResult:
            def result(self):
                return [{"processing_code": "ZZ"}, {"processing_code": "AA"}]

        service._run = MagicMock(return_value=_FakeResult())

        codes = service.list_compounding_how_codes()

        self.assertEqual(codes, ["ZZ", "AA"])

    def test_get_sku_summary_matches_pellet_bags_from_formulation_skus(self) -> None:
        # Ensure pellet bag lookup query uses formulation sku_list matching instead of tokenized location-code matching.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"
        service.get_ingredient = MagicMock(return_value={"sku": "ABC_001_25KG"})

        captured_queries = []

        class _FakeResult:
            def __init__(self, rows):
                self._rows = rows

            def result(self):
                return self._rows

        def fake_run(query, params):
            captured_queries.append(query)
            if "FROM `test-project.test_dataset.v_formulations_flat`" in query and "SELECT set_code" in query:
                return _FakeResult([])
            return _FakeResult([])

        service._run = fake_run

        service.get_sku_summary("ABC_001_25KG")

        self.assertTrue(any("UNNEST(f.sku_list)" in query for query in captured_queries))

    def test_status_option_normalization_exposes_received(self) -> None:
        # Ensure status list pages render canonical Received labels even when legacy source options include Recieved.
        options = get_allowed_status_options("long_moisture_status")

        self.assertIn("Received", options)
        self.assertNotIn("Recieved", options)

    def test_normalize_status_value_maps_legacy_spelling(self) -> None:
        # Normalize historical typo values so saves always persist canonical status text.
        self.assertEqual(normalize_status_value("Recieved"), "Received")

    def test_list_pellet_bags_with_meaningful_status_uses_density_assignee_fallback(self) -> None:
        # Ensure density dashboard pages map assigned_to from density assignee column before falling back to creator.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"

        captured = {}

        class _FakeResult:
            def result(self):
                return []

        def fake_run(query, params):
            captured["query"] = query
            return _FakeResult()

        service._run = fake_run

        service.list_pellet_bags_with_meaningful_status("density_status")

        self.assertIn("COALESCE(density_assignee_email, created_by)", captured["query"])

    def test_update_status_and_assignee_uses_whitelisted_mapping_fields(self) -> None:
        # Ensure inline status updates only touch one mapped status column and one mapped assignee column.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"

        captured = {}

        class _FakeResult:
            num_dml_affected_rows = 1

            def result(self):
                return []

        def fake_run(query, params):
            captured["query"] = query
            return _FakeResult()

        service._run = fake_run

        updated = service.update_pellet_bag_status_and_assignee(
            pellet_bag_code="PB-1",
            status_column="long_moisture_status",
            status_value="Received",
            assigned_value="qa@notpla.com",
            updated_by="qa@notpla.com",
        )

        self.assertIn("long_moisture_status = @status_value", captured["query"])
        self.assertIn("long_moisture_assignee_email = @assigned_value", captured["query"])
        self.assertEqual(updated["status_value"], "Received")


if __name__ == "__main__":
    unittest.main()
