from __future__ import annotations

import unittest

from google.cloud import bigquery

from app.services.bigquery_service import BigQueryService


class _FakeJob:
    def __init__(self, rows):
        # Store static result rows for the fake QueryJob.result() interface used by the service.
        self._rows = rows

    def result(self):
        # Return rows immediately to emulate an already-completed BigQuery query.
        return self._rows


class ListExistingBatchesTests(unittest.TestCase):
    def test_uses_sku_array_query_and_filters_exact_pairs_in_python(self) -> None:
        # Build a service instance without creating a real BigQuery client.
        service = object.__new__(BigQueryService)
        service.project_id = "project"
        service.dataset_id = "dataset"

        captured = {}

        def _fake_run(query, params):
            # Capture query + params so the test can assert no STRUCT array parameter is used anymore.
            captured["query"] = query
            captured["params"] = params
            # Return one exact match and one non-requested batch code for the same SKU.
            return _FakeJob(
                [
                    {"sku": "0_0000_00", "ingredient_batch_code": "BATCH_A"},
                    {"sku": "0_0000_00", "ingredient_batch_code": "OTHER"},
                ]
            )

        service._run = _fake_run  # type: ignore[method-assign]

        result = service.list_existing_batches(
            [("0_0000_00", "BATCH_A"), ("9_0279_1", "BATCH_B")]
        )

        self.assertEqual(result, {("0_0000_00", "BATCH_A")})
        self.assertIn("sku IN UNNEST(@skus)", captured["query"])
        self.assertEqual(len(captured["params"]), 1)
        self.assertIsInstance(captured["params"][0], bigquery.ArrayQueryParameter)
        self.assertEqual(captured["params"][0].name, "skus")

    def test_rejects_malformed_pairs_with_value_error(self) -> None:
        # Build a service instance without creating a real BigQuery client.
        service = object.__new__(BigQueryService)
        service.project_id = "project"
        service.dataset_id = "dataset"

        with self.assertRaises(ValueError):
            service.list_existing_batches([("only-one-value",)])  # type: ignore[list-item]


if __name__ == "__main__":
    unittest.main()
