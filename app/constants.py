"""Shared application constants used across API and UI integration points."""

# Global default page size for every paginated endpoint and table surface.
DEFAULT_PAGE_SIZE = 50

# Global upper bound to protect APIs from excessively large page requests.
MAX_PAGE_SIZE = 200

# Reusable formulation-set material workstream options keep dropdown values consistent across UI and API layers.
MATERIAL_WORKSTREAM_OPTIONS = [
    "Compounding",
    "Pellet Bags",
    "Conversion 1",
    "Injection Moulding",
    "Film Forming",
]

# Canonical failure-mode options shared by compounding and conversion workflows.
FAILURE_MODES = [
    "N/A",
    "Under-Plasticized",
    "Over-Plasticized",
    "Brittle Filament",
    "Powder Feed Block",
    "Liquid Feed Block",
    "Torque Limit",
    "Pressure Limit",
    "Barrel Blockage",
    "Unknown",
    "Sticky Film (direct to film)",
    "Brittle Film (direct to film)",
    "Heterogeneity",
]
