# XAPI.us — integration notes (unofficial)

This document summarizes the public documentation at [xapi.us/docs](https://xapi.us/docs) for use in this repo.

> Disclaimer: XAPI.us is a third‑party service that proxies Xbox-related data. Treat it as an external dependency with its own terms, quotas, and availability. Validate ToS/compliance before relying on it in production.

## Base URL

- API routes shown in the docs use the `https://xapi.us/api/...` prefix.

## Authentication

- Send an API token as a Bearer token:
  - `Authorization: Bearer <token>`

- If you receive `401 Unauthorized` **with a valid token**, the docs suggest your Xbox LIVE login may be stale.
  - Add `fresh-login` to the query string to force a refreshed login:
    - Example form: `https://xapi.us/api/<path>?fresh-login`

## Pagination (Continuation Tokens)

Some endpoints may return a Microsoft “Continuation Token”. When present:

- Preferred method: send it as a header:
  - `X-Continuation-Token: <ContinuationToken>`

- Alternate method: send it as a query parameter:
  - `continuation-token=<ContinuationToken>`

The docs explicitly note that continuation tokens are not always available and may not always work.

## Account selection (team plans)

If your team plan supports multiple accounts, you can select which account to use via a header:

- `x-account: <email>`

Notes from the docs:

- If the email is not a valid active account for the team, the API returns `422 Unprocessable Entity`.
- If omitted, the first active account is used.

## Rate limiting

XAPI.us emits rate-limit headers (per-minute) similar to:

- `x-ratelimit-limit`
- `x-ratelimit-remaining`

If you exceed limits, you may receive:

- HTTP `429 Too Many Requests`
- `retry-after: <seconds>`
- `x-ratelimit-reset: <unix_timestamp>`

The docs warn that ignoring these headers can result in IP blocking.

### Microsoft fine-grained rate limiting (still applies)

Even if you stay within XAPI.us limits, Microsoft has its own “fine‑grained rate limits” that can also produce HTTP 429.

Reference:

- [Microsoft Xbox Live Fine-Grained Rate Limiting (Implemented limits)](https://learn.microsoft.com/en-us/gaming/gdk/docs/services/develop/best-practices/live-fine-grained-rate-limiting#implemented-limits)

## Localization

- Default language is `en-US`.
- Override language by sending:
  - `Accept-Language: <locale>` (example shown: `en-GB`)

## Informational / response headers

The docs show additional headers such as:

- `x-team-name`
- `x-team-plan`
- `x-account-name`
- `x-account-email`

Treat these as informational response headers for diagnostics/telemetry.

## Xbox 360 achievement image URLs

The docs note that Xbox 360 achievement image URLs may need to be generated.

Pattern:

- `http://image.xboxlive.com/global/t.<titleid_in_hex>/ach/0/<imageId_in_hex>`

## Endpoint groups (high level)

The docs group endpoints into the following areas (no full per-route list was provided in the fetched HTML):

- Profile Specific (only works for the signed-in account)
- Profile
- Achievements
- Social
- Content
- Stats
- Marketplace
- Clubs

## Minimal request examples (token redacted)

- Authenticated GET:
  - `GET https://xapi.us/api/<route>`
  - `Authorization: Bearer <token>`

- With continuation token:
  - `X-Continuation-Token: <ContinuationToken>`

- With account selection:
  - `x-account: someone@example.com`

- With locale:
  - `Accept-Language: en-GB`
