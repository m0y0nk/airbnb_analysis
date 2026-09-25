# Source Map — FDE Ground Truth: Barcelona Airbnb Short-Term Rental Operations

## Business Problem & Information Needs

Leadership question: **"How available, expensive, and actively reviewed are Barcelona's Airbnb listings across neighbourhoods and weather conditions — and what operational decisions can we make from this data?"**

---

## Information Need → Source System Mapping

| # | Information Need | Required Fields | Source System | Format | Grain | System Owner | Freshness & Trust Level |
|---|---|---|---|---|---|---|---|
| 1 | **Listing profile & baseline attributes** | `id`, `host_id`, `neighbourhood_cleansed`, `latitude`, `longitude`, `room_type`, `price`, `last_scraped` | Inside Airbnb `listings.csv.gz` | Compressed CSV | 1 row per listing | Airbnb Platform / Supply Team | Snapshot at scrape date (2026-06-24); highly trusted for structural attributes |
| 2 | **Daily price & availability state** | `listing_id`, `date`, `available`, `price`, `adjusted_price`, `minimum_nights` | Inside Airbnb `calendar.csv.gz` | Compressed CSV | 1 row per listing × date | Reservations / Pricing Service | Forward-looking calendar view; high volume; **unavailability is ambiguous** (see Semantic Constraint) |
| 3 | **Guest review activity (stay proxy)** | `id`, `listing_id`, `date`, `reviewer_id` | Inside Airbnb `reviews.csv.gz` | Compressed CSV | 1 row per review event | Community / Trust & Safety Team | Event-driven historical record; reliable lower bound for completed stays |
| 4 | **Neighbourhood administrative hierarchy** | `neighbourhood_group`, `neighbourhood` | Inside Airbnb `neighbourhoods.csv` | CSV | 1 row per neighbourhood | Geographic Operations | Static reference; canonical taxonomy for Barcelona's 10 districts |
| 5 | **Geospatial neighbourhood boundaries** | `geometry` (Polygon/MultiPolygon), `neighbourhood` | Inside Airbnb `neighbourhoods.geojson` | GeoJSON | 1 feature per neighbourhood | GIS / Mapping Team | Static reference; authoritative spatial boundary file |
| 6 | **Historical daily weather context** | `time`, `temperature_2m_max`, `precipitation_sum`, `wind_speed_10m_max` | Open-Meteo REST API (`/v1/archive`) | JSON (REST) | 1 row per date (city centroid) | External Meteorological Service | Daily historical archive; queried for Barcelona centroid (41.3851°N, 2.1734°E) |

---

## Source System Descriptions

### Inside Airbnb
- **URL**: https://insideairbnb.com/get-the-data/
- **Snapshot**: Barcelona, 2026-06-24
- **Access Mode**: HTTP file download (GZ compressed CSV / GeoJSON)
- **Licensing**: Creative Commons CC0 (public domain)
- **Limitations**: No guest identifiers, no confirmed booking records, no revenue data

### Open-Meteo Historical Weather API
- **Endpoint**: `https://archive-api.open-meteo.com/v1/archive`
- **Access Mode**: REST API (GET, JSON response)
- **Parameters**: `latitude=41.3851`, `longitude=2.1734`, daily variables, date range
- **Spatial resolution**: Single city-centroid; does NOT capture microclimate variation across Barcelona's topography
- **Licensing**: CC BY 4.0

---

## System of Record (SoR) Decisions

| Domain | System of Record | Rationale |
|---|---|---|
| Listing identity & physical attributes | `listings.csv.gz` | Primary source for all listing-level metadata |
| Neighbourhood taxonomy | `neighbourhoods.csv` + `neighbourhoods.geojson` | Canonical administrative hierarchy |
| Calendar availability state & listed prices | `calendar.csv.gz` | SoR for daily state, **NOT for bookings or revenue** |
| Completed guest activity | `reviews.csv.gz` | Verified lower-bound proxy for stays (not a booking log) |
| Weather context | Open-Meteo API | Only authoritative external signal for meteorological data |

---

## ⚠️ Critical Semantic Constraint

> **Calendar unavailability (`available = 'f'`) does NOT equal a confirmed guest booking.**

A calendar date may be marked unavailable due to:
1. Host voluntary blockout (personal use, maintenance)
2. Platform-enforced minimum stay rule violations
3. Pending reservation not yet confirmed
4. Regulatory compliance hold

This pipeline models `is_unavailable` as an **OBSERVED STATE** (occupancy proxy).  
No field named `is_booked`, `confirmed_booking`, or `revenue` is created from calendar data alone.

---

## Gaps & Unknown Information

### Date-window decision for KPI4

KPI4 does not use the full forward-looking calendar horizon as historical weather data. The extraction step derives the weather request as follows:

1. Start at the earliest valid calendar date.
2. End at the earlier of the pipeline run date, the current date, or the calendar maximum date.
3. Inner-join daily calendar unavailability with weather by date.

For the current run, this produced **93 overlapping dates: 2026-06-24 through 2026-09-24**. The earlier two-row result came from a stale weather cache ending on 2026-06-25; insufficient cached coverage now triggers a refresh.

| Information | Status | Impact |
|---|---|---|
| Guest identity per booking | **Unknown** — not in Inside Airbnb | Cannot compute repeat booking rates |
| Host blockout dates vs. guest bookings | **Unknown** — indistinguishable in calendar | KPI 1 & 5 carry occupancy uncertainty |
| Actual accepted/paid price per stay | **Unknown** — only listed price available | KPI 5 is an upper-bound estimate |
| Daily calendar price | **Unavailable** in this snapshot | KPI2 is unsupported; KPI2B and KPI5 use listing-level price fallback where available |
| Instant-book vs. request-based listings | **Unknown** — not in public data | Booking conversion rate uncomputable |
| Guest demographics | **Unknown** | No segmentation possible |
