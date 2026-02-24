from __future__ import annotations

import unittest

from app.services.bigquery_service import BigQueryService


class IngredientSearchTests(unittest.TestCase):
    def test_search_query_is_case_insensitive(self) -> None:
        # Create a lightweight service instance without initialising a real BigQuery client.
        service = BigQueryService.__new__(BigQueryService)
        # Provide required attributes used by query builders.
        service.project_id = "project"
        service.dataset_id = "dataset"

        captured: dict[str, object] = {}

        # Stub _run to capture SQL and parameters while returning an empty result iterator.
        def fake_run(query: str, params):
            captured["query"] = query
            captured["params"] = params

            class _Job:
                # Return no rows because this test only validates filter construction.
                def result(self):
                    return []

            return _Job()

        # Inject fake query runner so no external BigQuery calls are made.
        service._run = fake_run  # type: ignore[method-assign]

        # Execute ingredient search with lower case input.
        service.list_ingredients({"q": "agar"})

        # Assert SQL uses lower-cased comparisons for case-insensitive matching.
        self.assertIn("LOWER(sku)", str(captured["query"]))
        # Assert bound query parameter is normalised to lower case wildcard search.
        self.assertEqual(captured["params"][0].value, "%agar%")


if __name__ == "__main__":
    unittest.main()
