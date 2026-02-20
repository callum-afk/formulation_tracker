from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from fastapi import HTTPException

from app.api.pellet_bags_api import INJECTION_FILM_STATUS_OPTIONS, _validate_optional_payload
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


if __name__ == "__main__":
    unittest.main()
