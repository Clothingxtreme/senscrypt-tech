# StreamTip Security Hardening

## Required Production Settings

Set these environment variables before deploying the hardened backend:

```env
ADMIN_TOKEN_QUERY_FALLBACK_ENABLED=false
SECURITY_RATE_LIMIT_ENABLED=true
TRUST_PROXY_HOPS=1
SECURITY_RATE_LIMIT_WINDOW_MS=900000
SECURITY_RATE_LIMIT_MAX=1200
AUTH_RATE_LIMIT_MAX=15
ADMIN_AUTH_RATE_LIMIT_MAX=8
SENSITIVE_ACTION_RATE_LIMIT_MAX=60
UPLOAD_RATE_LIMIT_MAX=25
WEBHOOK_RATE_LIMIT_MAX=600
IDENTITY_ENCRYPTION_KEY=replace-with-32-byte-random-hex-or-long-random-secret
PASSWORD_RESET_BASE_URL=https://streamtips.live
API_PUBLIC_BASE_URL=https://api.streamtips.live
KYC_DOCUMENT_SIGNING_SECRET=replace-with-different-long-random-secret
KYC_DOCUMENT_SIGNED_URL_TTL_SECONDS=900
```

Use a strong, backed-up `IDENTITY_ENCRYPTION_KEY`. If this key is lost, encrypted BVN/NIN values cannot be recovered.
Use a separate strong `KYC_DOCUMENT_SIGNING_SECRET` for temporary protected KYC document links. The portal/admin API returns signed document URLs so images stay on the API server while remaining unavailable as public raw files.

## Existing Identity Data Migration

After setting `IDENTITY_ENCRYPTION_KEY`, run:

```bash
npm run encrypt-identity
```

This encrypts existing plaintext BVN/NIN values in MongoDB. New or updated BVN/NIN values are encrypted automatically when the key is configured.

## Edge Protection

App-level rate limits reduce brute force and API abuse, but they do not stop large volumetric DDoS attacks. Put the API and frontend behind Cloudflare or another WAF/CDN with bot and DDoS protection enabled.
