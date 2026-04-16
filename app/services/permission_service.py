from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from fastapi import HTTPException, Request, status

# Define every supported permission in one place so route checks stay explicit and typo-safe.
PERMISSIONS = {
    "ingredients.view",
    "ingredients.edit",
    "sets.view",
    "sets.edit",
    "dry_weights.view",
    "dry_weights.edit",
    "batch_selection.view",
    "batch_selection.edit",
    "admin.user_roles.view",
    "admin.user_roles.edit",
    "dashboard.view",
    "utilities.view",
    "batches.view",
    "batches.edit",
    "location_codes.view",
    "location_codes.edit",
    "compounding_how.view",
    "compounding_how.edit",
    "pellet_bags.view",
    "pellet_bags.edit",
    "conversion1.view",
    "conversion1.edit",
    "status_lists.view",
    "status_lists.edit",
}

# Capture the SKU Codes role permissions as one explicit reusable set.
SKU_CODES_PERMISSIONS = {
    "batches.edit",
    "batches.view",
    "dashboard.view",
    "ingredients.edit",
    "ingredients.view",
    "pellet_bags.view",
    "sets.edit",
    "status_lists.edit",
    "status_lists.view",
    "utilities.view",
}
# Capture the Formulations role permissions as one explicit reusable set.
FORMULATIONS_PERMISSIONS = {
    "batch_selection.edit",
    "batch_selection.view",
    "batches.edit",
    "batches.view",
    "dashboard.view",
    "dry_weights.edit",
    "dry_weights.view",
    "ingredients.edit",
    "ingredients.view",
    "sets.view",
    "sets.edit",
    "status_lists.edit",
    "status_lists.view",
    "utilities.view",
}
# Capture the Formulations + Mix 1 role permissions as:
# 1) every Formulations permission, plus
# 2) the three Mixing 1 workflow pages requested by the user.
FORMULATIONS_MIX_PERMISSIONS = FORMULATIONS_PERMISSIONS | {
    "location_codes.edit",
    "location_codes.view",
    "compounding_how.edit",
    "compounding_how.view",
    "pellet_bags.edit",
    "pellet_bags.view",
}
# Capture the Mixing 1 role permissions as Formulations + the three dedicated mixing pages.
MIXING_1_PERMISSIONS = {
    "batch_selection.edit",
    "batch_selection.view",
    "batches.edit",
    "batches.view",
    "compounding_how.edit",
    "compounding_how.view",
    "dashboard.view",
    "dry_weights.edit",
    "dry_weights.view",
    "ingredients.edit",
    "ingredients.view",
    "location_codes.edit",
    "location_codes.view",
    "pellet_bags.edit",
    "pellet_bags.view",
    "sets.edit",
    "sets.view",
    "status_lists.edit",
    "status_lists.view",
    "utilities.view",
}
# Admin inherits every defined permission.
ADMIN_PERMISSIONS = set(PERMISSIONS)

# Map persisted role-group names to the concrete permission set used by route and content guards.
ROLE_GROUP_PERMISSIONS = {
    "sku_codes": SKU_CODES_PERMISSIONS,
    "formulations": FORMULATIONS_PERMISSIONS,
    "formulations_mix": FORMULATIONS_MIX_PERMISSIONS,
    "mixing_1": MIXING_1_PERMISSIONS,
    "admin": ADMIN_PERMISSIONS,
}

# Store canonical menu metadata centrally so the same config can drive sidebar filtering and route intent.
SIDEBAR_GROUPS = [
    {
        "id": "dashboard",
        "label": "General",
        "default_open": True,
        "items": [
            {"label": "Dashboard", "href": "/", "permission": "dashboard.view"},
        ],
    },
    {
        "id": "ingredients",
        "label": "Ingredients",
        "default_open": True,
        "items": [
            {"label": "Ingredient SKUs", "href": "/ingredients", "permission": "ingredients.view"},
            {"label": "Ingredient Batches", "href": "/batches", "permission": "batches.view"},
        ],
    },
    {
        "id": "formulation",
        "label": "Formulation",
        "default_open": True,
        "items": [
            {"label": "Formulation Sets", "href": "/sets", "permission": "sets.view"},
            {"label": "Dry Weights", "href": "/dry_weights", "permission": "dry_weights.view"},
            {"label": "Batch Selection", "href": "/batch_selection", "permission": "batch_selection.view"},
        ],
    },
    {
        "id": "mixing_1",
        "label": "Mixing 1",
        "default_open": True,
        "items": [
            {"label": "Mixing Location", "href": "/location_codes", "permission": "location_codes.view"},
            {"label": "Mixing How", "href": "/compounding_how", "permission": "compounding_how.view"},
            {"label": "Mixed Product", "href": "/pellet_bags", "permission": "pellet_bags.view"},
        ],
    },
    {
        "id": "system",
        "label": "System",
        "default_open": True,
        "items": [
            {"label": "Utilities", "href": "/utilities", "permission": "utilities.view"},
            {"label": "Conversion 1 Context", "href": "/conversion1/context", "permission": "conversion1.view"},
            {"label": "Conversion 1 How", "href": "/conversion1/how", "permission": "conversion1.view"},
            {"label": "Conversion 1 Products", "href": "/conversion1/products", "permission": "conversion1.view"},
        ],
    },
    {
        "id": "admin",
        "label": "Admin",
        "default_open": True,
        "items": [
            {"label": "User Roles", "href": "/admin/user-roles", "permission": "admin.user_roles.view"},
        ],
    },
]


@dataclass(frozen=True)
class ResolvedUserAccess:
    # Keep the persisted role row available for UI display and audit decisions.
    role_record: dict | None
    # Store the canonical resolved role-group name used by templates and server-side guards.
    role_group: str
    # Store the full resolved permission set for fast membership checks across one request.
    permissions: frozenset[str]
    # Flag admin access explicitly because templates and filters often need the shorthand.
    is_admin: bool
    # Flag bootstrap mode explicitly so the first signed-in user can seed access safely.
    is_bootstrap_admin: bool = False


def resolve_permissions_for_role(role_group: str | None) -> set[str]:
    # Return a copy so callers can extend safely without mutating the shared constant sets.
    return set(ROLE_GROUP_PERMISSIONS.get((role_group or "").strip().lower(), set()))


def build_sidebar_groups(permissions: Iterable[str], is_admin: bool) -> list[dict]:
    # Normalize to a set once so repeated permission checks stay cheap while filtering the menu tree.
    permission_set = set(permissions)
    filtered_groups: list[dict] = []
    for group in SIDEBAR_GROUPS:
        # Keep only menu items the current user may actually access server-side.
        visible_items = [
            {"label": item["label"], "href": item["href"]}
            for item in group["items"]
            if is_admin or item["permission"] in permission_set
        ]
        # Suppress empty groups entirely so hidden sections disappear from the sidebar.
        if visible_items:
            filtered_groups.append({**group, "items": visible_items})
    return filtered_groups


def get_request_access(request: Request) -> ResolvedUserAccess:
    # Reuse the request-scoped resolution injected by middleware so routes do not repeat BigQuery lookups.
    access = getattr(request.state, "user_access", None)
    if access is None:
        raise RuntimeError("User access has not been resolved for this request")
    return access


def has_permission(access: ResolvedUserAccess, permission: str) -> bool:
    # Admin always succeeds, while non-admin users need the specific named permission.
    return access.is_admin or permission in access.permissions


def require_permission(request: Request, permission: str) -> ResolvedUserAccess:
    # Resolve the request's permissions first so the caller can keep using the access object afterwards.
    access = get_request_access(request)
    if not has_permission(access, permission):
        # Return a hard 403 to enforce access control even when the route URL is guessed directly.
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    return access


def require_any_permission(request: Request, permissions: Sequence[str]) -> ResolvedUserAccess:
    # Allow routes to be opened by any one of several view permissions where appropriate.
    access = get_request_access(request)
    if access.is_admin or any(permission in access.permissions for permission in permissions):
        return access
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")


def can_view_dry_weights(access: ResolvedUserAccess) -> bool:
    # Dry-weight visibility is the critical high-sensitivity content gate requested by the user.
    return has_permission(access, "dry_weights.view")
