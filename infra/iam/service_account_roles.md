# Cloud Run service account roles

Required roles for the Cloud Run runtime service account:

- BigQuery Job User
- BigQuery Data Editor on the dataset
- Storage Object Viewer on `BUCKET_MSDS` and `BUCKET_SPECS`
- Service Account Token Creator on the signing service account (if using IAM signBlob for signed URLs)
