# IAP setup notes

- Enable IAP for the Cloud Run service.
- Ensure ingress settings are compatible with IAP (load balancer + IAP).
- Set `AUTH_MODE=iap` so the app expects `X-Goog-Authenticated-User-Email` headers.
- The app strips the `accounts.google.com:` prefix from the email header when present.

For Cloud Run IAM identity-token auth without IAP, set `AUTH_MODE=cloudrun`.
