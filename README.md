# FDE Ground Truth: Barcelona Airbnb Short-Term Rental Operations

> **Track C — Forward Deployment Engineering | Classes 4–8**  
> *From fragmented client data to a trustworthy operational pipeline*

---

## Problem Statement & Business Context

> Flasheats Assignment solution file: [assignment.md](assignment.md) — this is the solution file for the flasheats class assignment

Barcelona's short-term rental market is one of the most regulated and dynamic in Europe. Operational leadership needs a **dependable, explainable view** of how listing availability, nightly pricing, and guest activity evolve across neighbourhoods and environmental conditions — to guide acquisition strategy, pricing policy, and regulatory compliance planning.

This pipeline transforms raw, multi-source data from Inside Airbnb and Open-Meteo into **5 defensible operational KPIs**, stored in PostgreSQL and visualised in a standalone HTML dashboard.

### Core FDE Principle Applied
> *"The goal is not 'I analysed a dataset.' The goal is 'I built a trustworthy path from client systems to a business decision.'"*

---

## Stakeholders

| Stakeholder | Primary Question |
|---|---|
| **Regional Operations Manager** | Which neighbourhoods have the tightest supply (high unavailability)? |
| **Revenue Management Team** | How volatile are listed prices, and does weather shift pricing behaviour? |
| **Platform Growth Team** | Which room types and areas show the most guest engagement (review velocity)? |

---

## Source Overview

| Source | Format | Retrieval Mode | SoR For |
|---|---|---|---|
| `listings.csv.gz` | Compressed CSV | File download | Listing identity, baseline attributes |
| `calendar.csv.gz` | Compressed CSV | File download | Daily availability state + listed price |
| `reviews.csv.gz` | Compressed CSV | File download | Guest review events (stay proxy) |
| `neighbourhoods.csv` | CSV | File download | Neighbourhood taxonomy |
| `neighbourhoods.geojson` | GeoJSON | File download | Spatial boundaries |
| Open-Meteo API | JSON (REST) | REST API | Daily historical weather |

> **Snapshot**: Barcelona, Inside Airbnb (2026-06-24)

See [`docs/source_map.md`](docs/source_map.md) for full source mapping table.

---

## ⚠️ Critical Semantic Constraint

**Calendar unavailability ≠ confirmed booking.**

`available = 'f'` in calendar data may reflect:
- Host voluntary blockouts (personal use)
- Minimum-stay rule gaps
- Platform holds / pending reservations

All KPIs that use calendar data explicitly model `is_unavailable` as an **occupancy proxy**, not a verified booking event. No fabricated revenue or booking fields exist in this pipeline.

### Important source-date interpretation
The Barcelona Inside Airbnb download is dated 2026-06-24, but the raw `calendar.csv.gz` file contains a forward-looking availability horizon extending well beyond that point. This is expected for the source format: the release date identifies the snapshot, while the calendar is a rolling future schedule. For KPI interpretation, we therefore distinguish between:
- snapshot date = the source data collection date
- calendar horizon = the entire availability window in the raw file

Where date-window interpretation matters, KPIs should be explicitly tied to the chosen horizon rather than inferred as a single-day snapshot.

---

## Project KPI Scope

| KPI | Business Question | Formula | Limitation |
|---|---|---|---|
| **KPI 1** Unavailability Rate | What proportion of supply is off-market per neighbourhood/month? | `COUNT(is_unavailable=T) / total_days` | Includes blockouts; ≠ paid occupancy |
| **KPI 2** Price Volatility Index | How aggressively do hosts adjust rates? | `σ(price) / μ(price)` × 100 | Valid only when daily calendar pricing is present; otherwise use listing-level price baseline instead |
| **KPI 2B** Listing Price Baseline (fallback) | What is the prevailing listing-level price baseline by neighbourhood and room type? | `AVG(listing_price_usd)` and median | This is a fallback proxy when calendar prices are unavailable; not a true time-series volatility metric |
| **KPI 3** Review Velocity | What is the monthly completed-stay proxy rate? | `COUNT(reviews) per listing per month` | 60–70% of guests write reviews; lower bound |
| **KPI 4** Weather Sensitivity | Does extreme weather correlate with unavailability/rate drops? | `Corr(unavailability_rate, precipitation)` | Correlation ≠ causation |
| **KPI 5** Revenue Potential Proxy | Upper-bound revenue estimate by room type/neighbourhood? | `Σ(is_unavailable × proxy_price_usd)` | Uses calendar price when present, otherwise listing-level price; still an upper bound because unavailability is not a booking |

---

## Repository Structure

```
/
├── config/
│   └── config.yaml              # Pipeline configuration
├── docs/
│   ├── source_map.md            # Source Map
│   └── architecture_diagram.md  # Pipeline architecture
├── pipeline/
│   ├── __init__.py
│   ├── logger.py                # Structured JSON logger
│   ├── extract.py               # Multi-modal ingestion
│   ├── validate.py              # Business validation engine
│   ├── transform.py             # Clean + load to PostgreSQL
│   ├── kpi.py                   # KPI aggregation
│   └── run_pipeline.py          # CLI orchestrator
├── sql/
│   ├── 01_schema.sql            # PostgreSQL DDL
│   ├── 02_staging.sql           # Staging views & transforms
│   └── 03_kpis.sql              # KPI aggregation views
├── tests/
│   ├── test_retrieval.py        # API & download unit tests
│   └── test_validation.py       # Business rule unit tests
├── output/
│   ├── validation_report.json   # Pipeline validation output
│   ├── kpi1_unavailability_rate.csv
│   ├── kpi2_price_volatility.csv
│   ├── kpi3_review_velocity.csv
│   ├── kpi4_weather_sensitivity.csv
│   ├── kpi4_weather_correlations.csv
│   ├── kpi5_revenue_proxy.csv
│   ├── kpi_summary_dashboard.csv
│   ├── rejected/                 # quarantined invalid source rows + reasons
│   └── validation_report.json
├── dashboard/
│   └── index.html               # Standalone KPI dashboard
└── workspace/
    └── scratch/raw/{date}/      # Raw data preservation layer
```

### Final source-grounded findings

- The Inside Airbnb snapshot is dated **2026-06-24**, but its calendar is a forward-looking horizon through **2027-07-02**. Future calendar dates are retained and flagged rather than silently treated as historical observations.
- The calendar source has no daily `price` or `adjusted_price` fields. KPI2 therefore has no true daily volatility rows, while KPI2B provides a listing-level price baseline.
- KPI5 uses listing-level `price_usd` as a fallback for unavailable days when calendar pricing is absent. Rows without a price basis remain `NULL` rather than being fabricated as zero.
- KPI4 weather is requested from the earliest calendar date through the run date, capped at the current date. The current evidence covers **2026-06-24 through 2026-09-24 (93 days)** and joins only dates present in both sources.
- The validation report is intentionally `WARNING` with zero errors because it records source limitations and referential-quality issues for review.
- KPI5 now includes `priced_unavailable_days`, `unpriced_unavailable_days`, `price_coverage_pct`, and `price_basis`. A missing price basis is `NULL`, not a fabricated zero.
- Invalid or duplicate source rows are preserved under `output/rejected/` with `source_name` and `reject_reason`; their counts are also recorded in `validation_report.json`.
- File schemas, GeoJSON features, weather date continuity, requested weather range, and daily-array lengths are checked before transformation. Cached files used with `--skip-extract` receive the same structural checks.

---

## Setup & Execution

### Prerequisites

```bash
# Python dependencies
pip install pandas numpy requests pyyaml sqlalchemy psycopg2-binary pytest

# PostgreSQL (Homebrew / native)
brew install postgresql@14
brew services start postgresql@14

# Create database
createdb fde_ground_truth
```

### Run the Full Pipeline

```bash
# Full run (download + validate + transform + KPIs)
python pipeline/run_pipeline.py --run-date 2026-09-24

# Re-run using cached raw files (idempotent)
python pipeline/run_pipeline.py --run-date 2026-09-24 --skip-extract

# Validate + transform only (skip KPI aggregation)
python pipeline/run_pipeline.py --run-date 2026-09-24 --skip-kpi

# Custom config path
python pipeline/run_pipeline.py --config config/config.yaml
```

### Run Tests

```bash
pytest tests/ -v

# Optional PostgreSQL integration check: run the same date twice and compare
# output hashes, staging row counts, and primary-key uniqueness.
RUN_INTEGRATION=1 pytest tests/test_idempotency_integration.py -v
```

---

## Pipeline Architecture

```
[ Inside Airbnb Files ]           [ Open-Meteo REST API ]
 (CSV.GZ / GeoJSON)                    (JSON Endpoint)
           │                                 │
           └────────────────┬────────────────┘
                            ▼
                    ┌───────────────┐
                    │  Raw Storage  │  workspace/scratch/raw/{date}/
                    └───────┬───────┘
                            ▼
                    ┌───────────────┐
                    │  Validation   │  9 business rules, non-silent handling
                    └───────┬───────┘
                            ▼
                    ┌───────────────┐
                    │ Transformation│  Clean, parse, normalise
                    └───────┬───────┘
                            ▼
                    ┌───────────────┐
                    │  PostgreSQL   │  Relational schema + views
                    └───────┬───────┘
                            ▼
                    ┌───────────────┐
                    │  5 KPIs +     │  CSV evidence + HTML dashboard
                    │  Dashboard    │
                    └───────────────┘
```

---

## Business Decision Support

The KPI evidence table enables:

1. **Neighbourhood prioritisation**: KPI 1 + KPI 3 combined rank neighbourhoods by occupancy pressure and guest activity — guiding where to acquire new supply or tighten rental regulation.
2. **Dynamic pricing guidance**: KPI 2 (price volatility) identifies room types where hosts are most responsive to demand signals — supporting yield management recommendations.
3. **Weather-adjusted operations**: KPI 4 provides an evidence base for weather-conditional promotional strategy (e.g., discounting on high-rain weeks).

---

## Known / Unknown / Assumption / Limitation (KUAL)

| Category | Item |
|---|---|
| **Known** | Verified spatial boundaries, listing attributes, review timestamps, daily weather |
| **Known** | Barcelona snapshot date: 2026-06-24 (Inside Airbnb) |
| **Known** | Raw calendar file is a forward-looking availability horizon, not only a single-date snapshot |
| **Unknown** | Which unavailable days represent actual guest bookings vs. host blockouts |
| **Unknown** | Guest demographics, booking conversion rates, instant-book penetration |
| **Assumption** | Calendar unavailability is treated as an occupancy proxy (not a booking) |
| **Assumption** | Review timestamps lag stay completion by 1–5 days |
| **Assumption** | Open-Meteo centroid weather is representative of city-wide conditions |
| **Limitation** | Daily price-based KPIs are unsupported if the raw source does not supply daily price values |
| **Limitation** | KPI 5 is an **upper-bound estimate** — it overestimates true revenue |
| **Limitation** | Review velocity (KPI 3) captures ~60–70% of actual stays at best |
| **Limitation** | Weather correlation (KPI 4) does not control for confounders (tourism events, public holidays) |

---

## Important FDE Judgement Call

**Decision**: Model calendar unavailability strictly as an occupancy proxy rather than a booking indicator.

**Rationale**: Inside Airbnb's `calendar.csv` records `available = 'f'` for multiple reasons (blockouts, platform holds, minimum-stay gaps) that are indistinguishable from confirmed guest bookings without access to the actual reservation system. Conflating `available = 'f'` with revenue would produce inflated KPIs that mislead operational decisions. This pipeline makes this constraint explicit in:
- SQL column names (`is_unavailable`, not `is_booked`)
- KPI limitation documentation
- Dashboard warning labels

This is the epistemologically honest position for an FDE working with platform-extracted operational data.
