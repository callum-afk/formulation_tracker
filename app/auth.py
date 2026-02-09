from __future__ import annotations

import base64
import json
from dataclasses import dataclass

from fastapi import HTTPException, Request, status


@dataclass(frozen=True)
class AuthContext:
    email: str
    provider: str


def _normalize_iap_email(email: str) -> str:
    if ":" in email:
        _, email = email.split(":", 1)
    return email


def _parse_identity_token_payload(token: str) -> dict:
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid identity token format")

    payload_b64 = parts[1]
    padding = "=" * (-len(payload_b64) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload_b64 + padding)
        payload = json.loads(decoded.decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("Invalid identity token payload") from exc

    if not isinstance(payload, dict):
        raise ValueError("Invalid identity token payload")
    return payload


def _auth_from_iap_header(request: Request) -> AuthContext | None:
    iap_email = request.headers.get("X-Goog-Authenticated-User-Email")
    if not iap_email:
        return None
    return AuthContext(email=_normalize_iap_email(iap_email), provider="iap")


def _auth_from_bearer_token(request: Request) -> AuthContext | None:
    authorization = request.headers.get("Authorization")
    if not authorization:
        return None

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise ValueError("Missing Authorization bearer token")

    payload = _parse_identity_token_payload(token)
    email = payload.get("email") or payload.get("sub")
    if not email:
        raise ValueError("Missing email claim in identity token")
    return AuthContext(email=str(email), provider="cloudrun")


def get_auth_context(request: Request) -> AuthContext:
    auth_mode = request.app.state.settings.auth_mode

    if auth_mode == "none":
        return AuthContext(email="unknown", provider="none")

    if auth_mode == "iap":
        auth_context = _auth_from_iap_header(request)
        if auth_context:
            return auth_context
        raise ValueError("Not authenticated via IAP")

    if auth_mode == "cloudrun":
        auth_context = _auth_from_bearer_token(request)
        if auth_context:
            return auth_context
        raise ValueError("Missing Authorization bearer token")

    if auth_mode == "iap_or_cloudrun":
        auth_context = _auth_from_iap_header(request)
        if auth_context:
            return auth_context
        auth_context = _auth_from_bearer_token(request)
        if auth_context:
            return auth_context
        raise ValueError("Not authenticated")


    raise ValueError("Invalid AUTH_MODE configuration")


def require_auth_context(request: Request) -> AuthContext:
    try:
        return get_auth_context(request)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
