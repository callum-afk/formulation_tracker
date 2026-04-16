from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.services.bigquery_service import BigQueryService
from app.web import routes as web_routes


class SearchQueryTests(unittest.TestCase):
    def test_list_pellet_bags_adds_case_insensitive_search_filter(self) -> None:
        # Ensure pellet bag listing applies LOWER + LIKE SQL filtering when a search term is provided.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"
        captured = {}

        class _FakeResult:
            def result(self):
                return []

        def fake_run(query, params):
            captured["query"] = query
            captured["params"] = params
            return _FakeResult()

        service._run = fake_run

        service.list_pellet_bags(search="EV AB AL")

        self.assertIn("LOWER(pellet_bag_code) LIKE @search", captured["query"])
        self.assertEqual(captured["params"][0].value, "%ev ab al%")

    def test_list_compounding_how_adds_processing_code_filter(self) -> None:
        # Ensure compounding how listing supports suffix/code case-insensitive partial filtering.
        service = BigQueryService.__new__(BigQueryService)
        service.project_id = "test-project"
        service.dataset_id = "test_dataset"
        captured = {}

        class _FakeResult:
            def result(self):
                return []

        def fake_run(query, params):
            captured["query"] = query
            captured["params"] = params
            return _FakeResult()

        service._run = fake_run

        service.list_compounding_how(search="LX")

        self.assertIn("LOWER(processing_code) LIKE @search", captured["query"])
        self.assertIn("LOWER(process_code_suffix) LIKE @search", captured["query"])
        self.assertEqual(captured["params"][0].value, "%lx%")


class UserRolesSubmitTests(unittest.TestCase):
    def _build_client(self, bigquery: MagicMock) -> TestClient:
        # Build a focused FastAPI test app that mounts only web routes and injects minimal template/auth state.
        app = FastAPI()
        app.include_router(web_routes.router)
        app.state.settings = SimpleNamespace(app_version="test")

        # Override BigQuery dependency so tests can assert persistence calls without external services.
        app.dependency_overrides[web_routes.get_bigquery] = lambda: bigquery

        @app.middleware("http")
        async def add_request_state(request: Request, call_next):
            # Mimic authenticated admin state required by permission checks and shared base template rendering.
            request.state.user_email = "admin@example.com"
            request.state.sidebar_groups = []
            request.state.user_access = SimpleNamespace(
                is_admin=True,
                permissions={"admin.user_roles.view", "admin.user_roles.edit"},
                role_group="admin",
            )
            return await call_next(request)

        return TestClient(app)

    def test_user_role_update_accepts_json_payload(self) -> None:
        # Regress payload-shape bug by ensuring JSON request bodies are parsed and persisted correctly.
        bigquery = MagicMock()
        bigquery.list_user_roles.return_value = []
        client = self._build_client(bigquery)

        response = client.post(
            "/admin/user-roles",
            json={
                "email": "qa@example.com",
                "first_name": "QA",
                "last_name": "User",
                "role_group": "formulations_mix",
                "is_active": True,
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 303)
        bigquery.create_or_update_user_role.assert_called_once()
        _, kwargs = bigquery.create_or_update_user_role.call_args
        self.assertEqual(kwargs["email"], "qa@example.com")
        self.assertEqual(kwargs["role_group"], "formulations_mix")

    def test_batch_selection_detail_route_renders_when_record_exists(self) -> None:
        # Validate that the new batch-selection detail route resolves and renders linked payload data.
        bigquery = MagicMock()
        bigquery.list_user_roles.return_value = []
        bigquery.get_batch_selection_detail.return_value = {
            "base_code": "GE AX AB",
            "formulation": {"set_code": "GE", "weight_code": "AX", "batch_variant_code": "AB", "created_at": None, "created_by": "tester"},
            "location_codes": [],
            "compounding_how": [],
            "pellet_bags": [],
        }
        client = self._build_client(bigquery)

        response = client.get("/batch_selection/GE%20AX%20AB")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Batch Selection detail", response.text)


if __name__ == "__main__":
    unittest.main()
