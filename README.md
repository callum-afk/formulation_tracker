# Formulation Tracker

Formulation Tracker is a single Cloud Run service that serves both a FastAPI JSON API and server-rendered HTML (Jinja2) for managing ingredients, batches, sets, dry weight variants, and batch variants. It is designed to use BigQuery for domain data, Cloud Storage for PDF file storage, and IAP for authentication.

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

## Repository structure

- `app/`: FastAPI app and HTML templates
- `infra/`: BigQuery DDL, views, and infrastructure notes
- `.github/workflows/`: GitHub Actions deployment workflow

## Notes

- The service expects IAP headers for authenticated routes. When running locally, you can disable IAP enforcement by setting `DISABLE_AUTH=true` in the environment.
- BigQuery counters are implemented with an optimistic concurrency update loop to avoid collisions for low-concurrency internal usage.
