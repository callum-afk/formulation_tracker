from __future__ import annotations

from types import SimpleNamespace
import unittest

from fastapi.testclient import TestClient

from app.main import app


class RoutingSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        app.router.on_startup.clear()
        app.state.settings = SimpleNamespace(auth_mode="iap")

    def test_root_returns_html_with_iap_header(self) -> None:
        client = TestClient(app)

        response = client.get(
            "/",
            headers={"X-Goog-Authenticated-User-Email": "accounts.google.com:user@example.com"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers.get("content-type", ""))

    def test_health_is_json_without_auth(self) -> None:
        client = TestClient(app)

        response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})

    def test_static_is_public_but_ui_and_api_require_auth(self) -> None:
        client = TestClient(app)

        styles = client.get("/static/styles.css")
        script = client.get("/static/app.js")
        root = client.get("/")
        api = client.get("/api/ingredients")

        self.assertEqual(styles.status_code, 200)
        self.assertIn("text/css", styles.headers.get("content-type", ""))
        self.assertEqual(script.status_code, 200)
        self.assertIn("javascript", script.headers.get("content-type", ""))
        self.assertEqual(root.status_code, 401)
        self.assertEqual(api.status_code, 401)


if __name__ == "__main__":
    unittest.main()
