from __future__ import annotations

import unittest

from app.services.formulation_sanity import percentages_sum_to_100, sum_sku_percentages


class FormulationSanityTests(unittest.TestCase):
    def test_sum_sku_percentages_uses_all_sku_values(self) -> None:
        # Confirm AB AD-style payloads sum all SKU entries instead of repeating the first percent.
        payload = [
            {"sku": "0_0002_25", "wt_percent": 10},
            {"sku": "3_0001_25", "wt_percent": 10},
            {"sku": "3_0002_1", "wt_percent": 80},
        ]

        self.assertEqual(sum_sku_percentages(payload), 100.0)

    def test_percentages_sum_to_100_rejects_incorrect_total(self) -> None:
        # Guard against regressions where later SKU percentages are incorrectly duplicated.
        payload = [
            {"sku": "0_0002_25", "wt_percent": 10},
            {"sku": "3_0001_25", "wt_percent": 10},
            {"sku": "3_0002_1", "wt_percent": 10},
        ]

        self.assertFalse(percentages_sum_to_100(payload))


if __name__ == "__main__":
    unittest.main()
