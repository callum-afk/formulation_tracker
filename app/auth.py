from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import Header, HTTPException, status


@dataclass(frozen=True)
class IapUser:
    email: str
    user_id: Optional[str]


def parse_iap_headers(
    x_goog_authenticated_user_email: Optional[str],
    x_goog_authenticated_user_id: Optional[str],
) -> IapUser:
    if not x_goog_authenticated_user_email:
        raise ValueError("Missing IAP user email header")
    email = x_goog_authenticated_user_email
    if ":" in email:
        _, email = email.split(":", 1)
    return IapUser(email=email, user_id=x_goog_authenticated_user_id)


def require_iap_user(
    x_goog_authenticated_user_email: Optional[str] = Header(default=None),
    x_goog_authenticated_user_id: Optional[str] = Header(default=None),
) -> IapUser:
    try:
        return parse_iap_headers(
            x_goog_authenticated_user_email, x_goog_authenticated_user_id
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
