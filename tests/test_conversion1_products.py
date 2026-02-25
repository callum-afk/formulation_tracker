from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from fastapi import HTTPException

from app.api.conversion1_products_api import _validate_update_payload
from app.models import Conversion1ProductCreate
from app.services.bigquery_service import BigQueryService


class Conversion1ProductsTests(unittest.TestCase):
    def test_create_payload_normalizes_how_code_and_count_bounds(self) -> None:
        # Ensure create payload normalizes whitespace and preserves integer create-count validation.
        payload = Conversion1ProductCreate(conversion1_how_code=" AB   CD EF ", number_of_records=2)
        self.assertEqual(payload.conversion1_how_code, "AB CD EF")
        self.assertEqual(payload.number_of_records, 2)

    def test_update_validation_rejects_invalid_storage_location(self) -> None:
        # Ensure constrained storage-location enum rejects unknown values.
        with self.assertRaises(HTTPException):
            _validate_update_payload({"storage_location": "Unknown"}, update=True)

    def test_create_conversion1_products_allocates_global_suffix_range(self) -> None:
        # Verify create path uses globally shared suffix range and zero-padded product-code generation.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"
        service.allocate_conversion1_product_suffix_range = MagicMock(return_value=42)

        fake_job = MagicMock()
        fake_job.result.return_value = None
        service._run = MagicMock(return_value=fake_job)

        created = service.create_conversion1_products("EV AB", 2, "tester@notpla.com")

        service.allocate_conversion1_product_suffix_range.assert_called_once_with(2)
        self.assertEqual(created[0]["product_code"], "EV AB 0042")
        self.assertEqual(created[1]["product_code"], "EV AB 0043")

    def test_create_conversion1_products_accepts_optional_prefill_fields(self) -> None:
        # Verify create path forwards optional prefill values and keeps defaults for blank statuses.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"
        service.allocate_conversion1_product_suffix_range = MagicMock(return_value=7)

        fake_job = MagicMock()
        fake_job.result.return_value = None
        service._run = MagicMock(return_value=fake_job)

        service.create_conversion1_products(
            "EV AB",
            1,
            "tester@notpla.com",
            optional_fields={
                "notes": "sample note",
                "number_units_produced": 10,
                "tensile_rigid_status": "Ready",
            },
        )

        # Inspect first insert call parameters to confirm optional field bindings are populated.
        first_call_kwargs = service._run.call_args_list[0][0]
        query_parameters = first_call_kwargs[1]
        parameter_map = {parameter.name: parameter.value for parameter in query_parameters}
        self.assertEqual(parameter_map["notes"], "sample note")
        self.assertEqual(parameter_map["number_units_produced"], 10)
        self.assertEqual(parameter_map["tensile_rigid_status"], "Ready")
        # Confirm non-provided film tensile status inherits the tensile default fallback.
        self.assertEqual(parameter_map["tensile_films_status"], "Ready")


if __name__ == "__main__":
    unittest.main()
