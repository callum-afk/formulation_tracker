from __future__ import annotations

import base64
import json
import unittest
from dataclasses import dataclass

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from app.auth import get_auth_context


@dataclass
class TestSettings:
    auth_mode: str


def _identity_token(claims: dict) -> str:
    def _enc(data: dict) -> str:
        raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    return f"{_enc({'alg': 'RS256', 'typ': 'JWT'})}.{_enc(claims)}.signature"


def _build_app(auth_mode: str) -> FastAPI:
    app = FastAPI()
    app.state.settings = TestSettings(auth_mode=auth_mode)

    @app.middleware("http")
    async def auth_middleware(request: Request, call_next):
        try:
            get_auth_context(request)
        except ValueError as exc:
            return JSONResponse(status_code=401, content={"ok": False, "error": str(exc)})
        return await call_next(request)

    @app.get("/")
    async def root() -> dict:
        return {"ok": True}

    return app


class AuthModeTests(unittest.TestCase):
    def test_cloudrun_mode_accepts_identity_token(self) -> None:
        token = _identity_token({"email": "user@example.com"})
        client = TestClient(_build_app("cloudrun"))

        response = client.get("/", headers={"Authorization": f"Bearer {token}"})

        self.assertEqual(response.status_code, 200)

    def test_iap_mode_accepts_iap_header(self) -> None:
        client = TestClient(_build_app("iap"))

        response = client.get("/", headers={"X-Goog-Authenticated-User-Email": "accounts.google.com:user@example.com"})

        self.assertEqual(response.status_code, 200)

    def test_missing_auth_input_returns_401_with_error(self) -> None:
        client = TestClient(_build_app("cloudrun"))

        response = client.get("/")

        self.assertEqual(response.status_code, 401)
        self.assertIn("Missing Authorization bearer token", response.text)


if __name__ == "__main__":
    unittest.main()
