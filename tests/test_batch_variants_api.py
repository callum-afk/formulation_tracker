from __future__ import annotations

import unittest
from dataclasses import dataclass

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.api.batch_variants_api import router as batch_variants_router
from app.auth import AuthContext
from app.dependencies import get_actor, get_bigquery, get_settings
from app.services.permission_service import ResolvedUserAccess


@dataclass
class _Settings:
    code_start_batch: int = 0


class _FakeBigQuery:
    def __init__(self) -> None:
        # Track created batch variants so tests can assert the first-variant creation path is used.
        self.insert_calls: list[dict] = []

    def get_weight(self, set_code: str, weight_code: str) -> dict:
        # Return the IB AB dry-weight fixture with two SKUs and no existing batch variants.
        assert (set_code, weight_code) == ("IB", "AB")
        return {
            "set_code": "IB",
            "weight_code": "AB",
            "items": [
                {"sku": "0_0000_00", "wt_percent": 50.0},
                {"sku": "9_0279_1", "wt_percent": 50.0},
            ],
        }

    def list_existing_batches(self, pairs):
        # Simulate the duplicate-check source table where both requested ingredient batches exist.
        return set(pairs)

    def get_batch_variant_by_hash(self, set_code: str, weight_code: str, batch_hash: str):
        # Return None so API follows the create-first-variant flow.
        return None

    def allocate_counter(self, counter_name: str, scope: str, start_value: int) -> int:
        # Return a deterministic counter so assertions can validate the generated batch variant code.
        assert counter_name == "batch_variant_code"
        assert scope == "IB AB"
        assert start_value == 0
        return 0

    def insert_batch_variant(self, set_code, weight_code, batch_variant_code, batch_hash, items, created_by):
        # Capture insert input for assertion and emulate successful persistence.
        self.insert_calls.append(
            {
                "set_code": set_code,
                "weight_code": weight_code,
                "batch_variant_code": batch_variant_code,
                "items": list(items),
                "created_by": created_by,
            }
        )


def _build_test_client(fake_bigquery: _FakeBigQuery) -> TestClient:
    # Build an isolated API app so this suite can test the route behavior without touching production dependencies.
    app = FastAPI()
    app.include_router(batch_variants_router)

    @app.middleware("http")
    async def inject_access(request: Request, call_next):
        # Inject permissions required by POST /api/batch_variants into request.state for route authorization.
        request.state.user_access = ResolvedUserAccess(
            role_record=None,
            role_group="formulations",
            permissions=frozenset({"batch_selection.edit", "batch_selection.view"}),
            is_admin=False,
            is_bootstrap_admin=False,
        )
        return await call_next(request)

    # Override service dependencies with deterministic test doubles.
    app.dependency_overrides[get_bigquery] = lambda: fake_bigquery
    app.dependency_overrides[get_settings] = lambda: _Settings()
    app.dependency_overrides[get_actor] = lambda: AuthContext(email="tester@example.com", provider="none")
    return TestClient(app)


class BatchVariantsApiTests(unittest.TestCase):
    def test_create_first_batch_variant_for_ib_ab_succeeds(self) -> None:
        # Verify the regression scenario: IB AB has valid dry-weight data and zero existing batch variants.
        fake_bigquery = _FakeBigQuery()
        client = _build_test_client(fake_bigquery)

        response = client.post(
            "/api/batch_variants",
            json={
                "set_code": "IB",
                "weight_code": "AB",
                "items": [
                    {"sku": "0_0000_00", "ingredient_batch_code": "BATCH_A"},
                    {"sku": "9_0279_1", "ingredient_batch_code": "BATCH_B"},
                ],
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["batch_variant_code"], "AA")
        self.assertEqual(len(fake_bigquery.insert_calls), 1)
        self.assertEqual(
            fake_bigquery.insert_calls[0]["items"],
            [("0_0000_00", "BATCH_A"), ("9_0279_1", "BATCH_B")],
        )

    def test_create_batch_variant_rejects_blank_batch_code(self) -> None:
        # Confirm malformed payloads return validation errors and never reach BigQuery duplicate checks.
        fake_bigquery = _FakeBigQuery()
        client = _build_test_client(fake_bigquery)

        response = client.post(
            "/api/batch_variants",
            json={
                "set_code": "IB",
                "weight_code": "AB",
                "items": [
                    {"sku": "0_0000_00", "ingredient_batch_code": "   "},
                ],
            },
        )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(fake_bigquery.insert_calls, [])


if __name__ == "__main__":
    unittest.main()
