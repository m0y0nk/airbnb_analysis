# Pipeline Architecture — FDE Ground Truth: Barcelona Airbnb

## End-to-End Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (Multi-Modal Retrieval)                     │
│                                                                                 │
│  ┌───────────────────────────────┐    ┌────────────────────────────────────┐    │
│  │    Inside Airbnb CDN          │    │    Open-Meteo REST API              │    │
│  │                               │    │                                    │    │
│  │  listings.csv.gz  ──────────► │    │  GET /v1/archive                   │    │
│  │  calendar.csv.gz  ──────────► │    │  lat=41.3851, lon=2.1734           │    │
│  │  reviews.csv.gz   ──────────► │    │  daily vars: temp, precip, wind    │    │
│  │  neighbourhoods.csv ────────► │    │                                    │    │
│  │  neighbourhoods.geojson ────► │    │  Retry: exponential backoff        │    │
│  │                               │    │  Retries: 4x (429/500/503/504)     │    │
│  └───────────────┬───────────────┘    └──────────────────┬─────────────────┘    │
└──────────────────┼───────────────────────────────────────┼─────────────────────┘
                   │                                       │
                   ▼                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         RAW PRESERVATION LAYER                                  │
│                    workspace/scratch/raw/{run-date}/                             │
│                                                                                 │
│   listings.csv.gz  calendar.csv.gz  reviews.csv.gz                             │
│   neighbourhoods.csv  neighbourhoods.geojson  weather_barcelona.json            │
│                                                                                 │
│   ✓ Date-partitioned: each run-date has its own folder (idempotent)            │
│   ✓ Byte-size check: 0-byte files halt pipeline immediately                    │
│   ✓ JSON key check: 'daily', 'time' keys required in weather payload           │
│   ✓ Schema checks: required CSV columns and valid GeoJSON features              │
│   ✓ Weather checks: exact requested range, contiguous dates, array lengths      │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                      VALIDATION ENGINE (12 Business Rules)                      │
│                                                                                 │
│   FATAL (halts pipeline):                                                       │
│     V01: Duplicate listing IDs > 0.1% threshold                                │
│     V02: Coordinates outside lat[-90,90] / lon[-180,180]                       │
│     V07: Orphaned reviews > 5% threshold                                       │
│                                                                                 │
│   WARNING (pipeline continues, records flagged):                                │
│     V03: Missing price on available calendar days                               │
│     V04: Duplicate (listing_id, date) composite keys                           │
│     V05: Calendar dates before 2020-01-01                                      │
│     V06: Review dates in the future                                             │
│     V07: Orphaned reviews within tolerance                                      │
│     V08: Calendar rows referencing unknown listings                             │
│     V09: Fewer than 30 weather days                                             │
│     V10: Calendar horizon extends beyond run date                               │
│     V11: Calendar has no daily pricing                                          │
│     V12: Listing-level price fallback availability                              │
│                                                                                 │
│   OUTPUT: output/validation_report.json                                         │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                     TRANSFORMATION & NORMALISATION                              │
│                                                                                 │
│   Listings:    Strip $/, parse price; deduplicate on id                        │
│   Calendar:    Parse dates; strip $/, parse price; is_unavailable = (avail='f')│
│   Reviews:     Validate dates; tag orphaned=TRUE for referential failures       │
│   Weather:     Flatten JSON → row-per-date DataFrame                           │
│   Neighbourhoods: Normalise column names                                        │
│                                                                                 │
│   ⚠ Semantic constraint: is_unavailable is an observed state, NOT a booking    │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         POSTGRESQL STAGING (sql/01 + 02)                        │
│                                                                                 │
│   stg_listings (15K rows)      stg_calendar (5.6M rows)                        │
│   stg_reviews  (1M rows)       stg_weather  (93 rows in current run)            │
│   stg_neighbourhoods (73 rows)                                                  │
│                                                                                 │
│   Idempotency: TRUNCATE CASCADE before each load                               │
│   Views: v_calendar_enriched, v_reviews_validated, v_weather_calendar_joined   │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           KPI AGGREGATION (sql/03)                              │
│                                                                                 │
│   KPI 1: Unavailability Rate      (neighbourhood × month)                      │
│   KPI 2: Price Volatility CoV     (room_type × neighbourhood × month)          │
│   KPI 3: Review Velocity          (neighbourhood × month)                      │
│   KPI 4: Weather Sensitivity      (overlapping city × date) + correlations     │
│   KPI 5: Revenue Proxy            (listing-price fallback × month)              │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              OUTPUT LAYER                                       │
│                                                                                 │
│   output/validation_report.json          (pipeline QA + reject counts)         │
│   output/rejected/*.csv                  (quarantined rows + reasons)          │
│   output/kpi1_unavailability_rate.csv                                           │
│   output/kpi2_price_volatility.csv                                              │
│   output/kpi3_review_velocity.csv                                               │
│   output/kpi4_weather_sensitivity.csv                                           │
│   output/kpi4_weather_correlations.csv                                          │
│   output/kpi5_revenue_proxy.csv                                                 │
│   output/kpi_summary_dashboard.csv      (evidence table)                       │
│   dashboard/index.html                  (interactive KPI dashboard)             │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Idempotency Guarantees

| Stage | Idempotency Mechanism |
|---|---|
| Raw download | Skip if file exists and size > 0 |
| Weather API | Cache raw JSON; skip if file exists |
| PostgreSQL staging | `TRUNCATE … CASCADE` before every load |
| KPI views | `CREATE OR REPLACE VIEW` |
| Evidence CSVs | Overwritten on each run |

## Error Handling

| Error Type | Response |
|---|---|
| HTTP 429/500/502/503/504 | Exponential backoff (max 4 retries, up to 16s wait) |
| HTTP 400/401/403/404 | Immediate `RuntimeError` — no retry |
| 0-byte downloaded file | `RuntimeError` — halts extraction stage |
| Missing weather JSON keys | `RuntimeError` — halts weather stage |
| FATAL validation rule | `sys.exit(1)` with structured log |
| DB connectivity failure | SQLAlchemy raises, orchestrator logs and exits |

## Structured Logging

Every pipeline step emits JSON log lines:
```json
{"ts":"2026-09-24T12:42:00.123Z","level":"INFO","step":"transform.load","msg":"Loaded 15,293 rows → stg_listings","meta":{"rows":15293}}
```

Fields: `ts` (ISO8601), `level` (INFO/WARNING/ERROR), `step` (module.substep), `msg`, optional `meta` dict.
