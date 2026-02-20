#!/usr/bin/env python3
"""Reset and rebuild the application's BigQuery dataset schema."""

# Import argparse so the script can expose safe CLI flags for destructive behavior.
import argparse
# Import os to read Cloud Run environment variables for project and dataset values.
import os
# Import sys so the script can return explicit process exit codes.
import sys
# Import Path to resolve SQL files from the repository consistently.
from pathlib import Path
# Import Optional to annotate helper function return types clearly.
from typing import Optional

# Import BigQuery client classes used to delete, create, and query dataset resources.
from google.cloud import bigquery


# Define the repository root relative to this script so SQL path resolution stays deterministic.
REPO_ROOT = Path(__file__).resolve().parent.parent
# Define where ordered DDL migrations are stored.
DDL_DIR = REPO_ROOT / "infra" / "bigquery" / "ddl"
# Define where view SQL files are stored.
VIEWS_DIR = REPO_ROOT / "infra" / "bigquery" / "views"


# Build CLI arguments so operators can confirm destructive actions when running in Cloud Run jobs.
def parse_args() -> argparse.Namespace:
    # Create a top-level parser with a clear description for logs and --help output.
    parser = argparse.ArgumentParser(description="Delete and rebuild BigQuery dataset schema.")
    # Require a safety flag to avoid accidental data loss from unintended command execution.
    parser.add_argument(
        "--confirm-destroy",
        action="store_true",
        help="Required flag that acknowledges all dataset contents will be deleted first.",
    )
    # Allow an optional location override while still supporting environment defaults.
    parser.add_argument(
        "--location",
        default=None,
        help="BigQuery location override (default: BQ_LOCATION, then REGION).",
    )
    # Return parsed arguments to the caller.
    return parser.parse_args()


# Resolve the BigQuery location from CLI and environment in a predictable precedence order.
def resolve_location(cli_location: Optional[str]) -> Optional[str]:
    # Iterate through candidates in priority order so explicit CLI input always wins.
    for candidate in (cli_location, os.getenv("BQ_LOCATION"), os.getenv("REGION")):
        # Return the first non-empty trimmed value to avoid malformed blank settings.
        if candidate and candidate.strip():
            return candidate.strip()
    # Return None when no location is configured so BigQuery defaults can apply.
    return None


# Read one SQL file, replace placeholders, split statements, and execute sequentially.
def run_sql_file(client: bigquery.Client, location: Optional[str], sql_file: Path, project_id: str, dataset_id: str) -> None:
    # Load file contents as UTF-8 text so migrations remain portable across environments.
    raw_sql = sql_file.read_text(encoding="utf-8")
    # Replace template placeholders used by repository SQL files.
    rendered_sql = raw_sql.replace("PROJECT_ID", project_id).replace("DATASET_ID", dataset_id)
    # Execute each statement independently so multi-statement files are handled correctly.
    for statement in (part.strip() for part in rendered_sql.split(";") if part.strip()):
        # Submit the statement as a query job targeted at the configured location.
        job = client.query(statement, location=location)
        # Block until the statement finishes so ordering is preserved.
        job.result()


# Run all baseline and additive SQL files in deterministic order after dataset recreation.
def rebuild_schema(client: bigquery.Client, location: Optional[str], project_id: str, dataset_id: str) -> None:
    # Define mandatory baseline files that must run first for table and seed initialization.
    baseline_files = [
        DDL_DIR / "001_create_tables.sql",
        DDL_DIR / "002_seed_counters.sql",
        VIEWS_DIR / "101_views.sql",
    ]
    # Execute each baseline file when present.
    for sql_file in baseline_files:
        # Skip missing files gracefully so the script can still run in partially customized repos.
        if sql_file.exists():
            print(f"Applying baseline SQL: {sql_file.relative_to(REPO_ROOT)}")
            run_sql_file(client, location, sql_file, project_id, dataset_id)

    # Track baseline names to avoid re-running them in the additive migration loop.
    baseline_names = {path.name for path in baseline_files}
    # Execute all additional DDL migrations in lexical order for deterministic upgrades.
    for sql_file in sorted(DDL_DIR.glob("*.sql")):
        # Skip baseline files that already ran.
        if sql_file.name in baseline_names:
            continue
        print(f"Applying additive SQL: {sql_file.relative_to(REPO_ROOT)}")
        run_sql_file(client, location, sql_file, project_id, dataset_id)


# Orchestrate dataset deletion, recreation, and schema rebuild operations.
def main() -> int:
    # Parse command-line arguments once at startup.
    args = parse_args()

    # Enforce explicit confirmation for destructive actions before touching BigQuery.
    if not args.confirm_destroy:
        print("ERROR: --confirm-destroy is required because this operation deletes dataset contents.", file=sys.stderr)
        return 2

    # Read required project and dataset settings from environment variables.
    project_id = os.getenv("PROJECT_ID", "").strip()
    dataset_id = os.getenv("DATASET_ID", "").strip()

    # Validate required project identifier and fail fast with a clear message when missing.
    if not project_id:
        print("ERROR: PROJECT_ID environment variable is required.", file=sys.stderr)
        return 2

    # Validate required dataset identifier and fail fast with a clear message when missing.
    if not dataset_id:
        print("ERROR: DATASET_ID environment variable is required.", file=sys.stderr)
        return 2

    # Resolve location once so every operation targets the same region.
    location = resolve_location(args.location)
    # Build the dataset reference string used for all dataset-level API calls.
    dataset_ref = f"{project_id}.{dataset_id}"

    # Create a BigQuery client scoped to the configured project.
    client = bigquery.Client(project=project_id)

    # Log the destructive action so Cloud Run job logs clearly show what happened.
    print(f"Deleting dataset (if it exists): {dataset_ref}")
    # Delete the dataset and all of its objects; ignore not-found to support first-time runs.
    client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)

    # Create a fresh dataset object so schema files can be applied from scratch.
    dataset = bigquery.Dataset(dataset_ref)
    # Assign location when one is configured so dataset creation stays region-consistent.
    if location:
        dataset.location = location

    # Log dataset creation details for observability in Cloud Run job execution output.
    print(f"Creating dataset: {dataset_ref} (location={location or 'default'})")
    # Create the new dataset and fail if creation cannot complete.
    client.create_dataset(dataset, exists_ok=False)

    # Reapply all schema and view SQL files after dataset recreation.
    rebuild_schema(client, location, project_id, dataset_id)

    # Print success output so operators can quickly see completion in logs.
    print("BigQuery dataset rebuild completed successfully.")
    # Return a zero exit code for successful job completion.
    return 0


# Run the main routine when this file is executed directly as a command.
if __name__ == "__main__":
    # Exit with the returned code so Cloud Run job status reflects script success or failure.
    raise SystemExit(main())
