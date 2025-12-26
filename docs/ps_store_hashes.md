# PlayStation Store Persisted Query Hashes

Captured via Chrome DevTools on 2025-11-18.

## Locale: en-GB

| Operation | Context | SHA-256 Hash |
| --- | --- | --- |
| `categoryGridRetrieve` | Storefront browse grid, offset=24 ("Coffee Talk Tokyo" tile) | `2577134666fc3264850aa473409a29088e3a4115e6e69e9fb3e061c8dd5b9f5c6` |
| `queryOracleUserProfileFullSubscription` | Bootstrap subscription/profile payload | `3fe5e3cb6e16f83be98ccaa694823e10bdf428f9c7ff8e314b8464ad8976319d` |
| `queryRetrieveTelemetryDataPDPConcept` (`conceptId=10012616`) | PDP telemetry payload for Coffee Talk Tokyo | `906eef4a71644e7c6c9c7b300b68754e385f01149240c1767bbca493c5d92485` |
| `wcaConceptStarRatingRetrive` (`conceptId=10012616`) | Star rating widget data | `8c3dea41cf2f56baf3e0e0bfdf5e7298fa2941ab7488b8d7859bb0200dfb99b9` |
| `conceptRetrieveForCtasWithPrice` (`conceptId=10012616`) | CTA block with PS Plus upsell pricing | `ad436289be51f8af3c232d60811d6a91ff26ba2f5668f6d5f1adcdd7e3447f32` |
| `conceptRetrieveForUpsellWithCtas` (`conceptId=10012616`) | Upsell CTA metadata | `8109a9596c0b1172e5ef8ad7c7938ffafb627b3e70d899bf6637398c9b802d3c` |

> Source headers confirmed `x-psn-store-locale-override: en-GB`.

## Locale: en-US

Pending capture. Use Chrome DevTools → Network while browsing PS5/PS4 category grids and PDPs. Copy `operationName` and `extensions.persistedQuery.sha256Hash` from requests to `web.np.playstation.com/api/graphql/v1/op`.

Tracked ops (expected):

- `categoryGridRetrieve` → sha256Hash: (TBD)
- `queryOracleUserProfileFullSubscription` → sha256Hash: (TBD)
- `conceptRetrieveForCtasWithPrice` → sha256Hash: (TBD)

## Locale: de-DE

Pending capture. Repeat the same procedure for the German locale.

- `categoryGridRetrieve` → sha256Hash: (TBD)
- `queryOracleUserProfileFullSubscription` → sha256Hash: (TBD)
- `conceptRetrieveForCtasWithPrice` → sha256Hash: (TBD)

## Client override file

Hashes are mirrored into `psstore_client/hashes.json` keyed by locale and operationName. The PS client should consult this map before falling back to built-ins.
