# IAP setup notes

- Enable IAP for the Cloud Run service.
- Ensure ingress settings are compatible with IAP (load balancer + IAP).
- The app expects `X-Goog-Authenticated-User-Email` and `X-Goog-Authenticated-User-Id` headers.

Local development can set `DISABLE_AUTH=true` to bypass IAP checks.
