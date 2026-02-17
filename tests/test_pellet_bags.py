from __future__ import annotations

import unittest

from fastapi import HTTPException

from app.api.pellet_bags_api import INJECTION_FILM_STATUS_OPTIONS, _validate_optional_payload
from app.models import PelletBagCreate


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


if __name__ == "__main__":
    unittest.main()
