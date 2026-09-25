-- =============================================================================
-- 02_staging.sql — Staging Transforms & Core Relational Tables
-- =============================================================================
-- Runs after Python loads data into stg_* tables.
-- Creates clean analytical views for KPI queries.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- View: v_active_listings
-- Listings with at least 30 days of calendar coverage (KPI filter threshold).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_active_listings AS
SELECT
    l.*,
    n.neighbourhood_group
FROM stg_listings l
LEFT JOIN stg_neighbourhoods n ON n.neighbourhood = l.neighbourhood
WHERE l.id IN (
    SELECT listing_id
    FROM stg_calendar
    GROUP BY listing_id
    HAVING COUNT(*) >= 30
);

-- ---------------------------------------------------------------------------
-- View: v_calendar_enriched
-- Joins calendar with listing metadata; preserves semantic constraint.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_calendar_enriched AS
SELECT
    c.listing_id,
    c.date,
    c.is_unavailable,         -- OCCUPANCY PROXY, not a booking confirmation
    c.price_usd,
    c.adjusted_price_usd,
    c.minimum_nights          AS cal_minimum_nights,
    l.neighbourhood,
    l.room_type,
    n.neighbourhood_group,
    EXTRACT(YEAR  FROM c.date)::INTEGER AS year,
    EXTRACT(MONTH FROM c.date)::INTEGER AS month_num,
    TO_CHAR(c.date, 'YYYY-MM')          AS year_month,
    l.price_usd                         AS listing_price_usd
FROM stg_calendar c
JOIN stg_listings l ON l.id = c.listing_id
LEFT JOIN stg_neighbourhoods n ON n.neighbourhood = l.neighbourhood;

-- ---------------------------------------------------------------------------
-- View: v_reviews_validated
-- Reviews restricted to non-orphaned records only.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_reviews_validated AS
SELECT
    r.*,
    l.neighbourhood,
    l.room_type,
    n.neighbourhood_group,
    TO_CHAR(r.date, 'YYYY-MM') AS year_month
FROM stg_reviews r
JOIN stg_listings l ON l.id = r.listing_id
LEFT JOIN stg_neighbourhoods n ON n.neighbourhood = l.neighbourhood
WHERE r.orphaned = FALSE;

-- ---------------------------------------------------------------------------
-- View: v_weather_calendar_joined
-- Daily city-level unavailability rate joined to weather observations.
-- Used for KPI 4 (Weather Sensitivity Index).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_weather_calendar_joined AS
WITH daily_unavail AS (
    SELECT
        date,
        COUNT(*)                                                AS total_listings,
        SUM(CASE WHEN is_unavailable THEN 1 ELSE 0 END)        AS unavailable_count,
        ROUND(
            100.0 * SUM(CASE WHEN is_unavailable THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*), 0),
            4
        )                                                       AS unavailability_rate_pct
    FROM stg_calendar
    WHERE date IS NOT NULL
    GROUP BY date
)
SELECT
    d.date,
    d.total_listings,
    d.unavailable_count,
    d.unavailability_rate_pct,
    w.temp_max_c,
    w.precipitation_mm,
    w.wind_speed_max_kmh
FROM daily_unavail d
JOIN stg_weather w ON w.date = d.date
ORDER BY d.date;
