# Formulation Tracker

Formulation Tracker is a single Cloud Run service that serves both a FastAPI JSON API and server-rendered HTML (Jinja2) for managing ingredients, batches, sets, dry weight variants, and batch variants. It is designed to use BigQuery for domain data, Cloud Storage for PDF file storage, and Cloud Run/IAP-based authentication.

## Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Environment variables

Required:
- `PROJECT_ID`
- `DATASET_ID`
- `REGION`
- `BUCKET_MSDS`
- `BUCKET_SPECS`
- `CLOUD_RUN_SERVICE_NAME`

Optional:
- `LOG_LEVEL`
- `CODE_START_SET` (default `1` for `AB`)
- `CODE_START_WEIGHT` (default `1` for `AB`)
- `CODE_START_BATCH` (default `1` for `AB`)
- `AUTH_MODE` (`cloudrun`, `iap`, or `none`; default `cloudrun`)

## Repository structure

- `app/`: FastAPI app and HTML templates
- `infra/`: BigQuery DDL, views, and infrastructure notes
- `.github/workflows/`: GitHub Actions deployment workflow

## Notes

- `AUTH_MODE=cloudrun` expects an `Authorization: Bearer <identity-token>` header and reads `email` (or `sub`) from the JWT payload.
- `AUTH_MODE=iap` expects `X-Goog-Authenticated-User-Email` and remains compatible with IAP/header-based auth.
- `AUTH_MODE=none` allows unauthenticated access and uses an `unknown` actor for audit fields.
- For Cloud Run deployment, ensure `AUTH_MODE=cloudrun` plus required vars: `PROJECT_ID`, `DATASET_ID`, `BUCKET_MSDS`, `BUCKET_SPECS`, and `CLOUD_RUN_SERVICE_NAME`.
- BigQuery counters are implemented with an optimistic concurrency update loop to avoid collisions for low-concurrency internal usage.
