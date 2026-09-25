-- =============================================================================
-- 03_kpis.sql — KPI Aggregation Views
-- =============================================================================
-- These views materialise the 5 core operational KPIs.
-- They are also run as Python SQL queries in pipeline/kpi.py.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- KPI 1: Adjusted Listing Unavailability Rate (Occupancy Proxy)
-- Grain: Neighbourhood × Month
-- LIMITATION: Unavailability includes host blockouts, maintenance holds,
-- and minimum-stay rule gaps. It does NOT equal confirmed paid occupancy.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW kpi1_unavailability_rate AS
SELECT
    neighbourhood,
    DATE_TRUNC('month', date)::DATE             AS month,
    COUNT(*)                                    AS total_days,
    SUM(CASE WHEN is_unavailable THEN 1 ELSE 0 END)  AS unavailable_days,
    ROUND(
        100.0 * SUM(CASE WHEN is_unavailable THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        2
    )                                           AS unavailability_rate_pct,
    COUNT(DISTINCT listing_id)                  AS active_listings
FROM v_calendar_enriched
GROUP BY neighbourhood, DATE_TRUNC('month', date)
HAVING COUNT(*) >= 30
ORDER BY neighbourhood, month;

-- ---------------------------------------------------------------------------
-- KPI 2: Listed Price Volatility Index (Coefficient of Variation)
-- Grain: Neighbourhood × Room Type × Month
-- LIMITATION: Reflects listed prices, not accepted/negotiated booking rates.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW kpi2_price_volatility AS
SELECT
    neighbourhood,
    room_type,
    DATE_TRUNC('month', date)::DATE             AS month,
    COUNT(DISTINCT listing_id)                  AS listings_included,
    ROUND(AVG(price_usd)::NUMERIC, 2)           AS avg_listed_price_usd,
    ROUND(STDDEV(price_usd)::NUMERIC, 2)        AS stddev_price_usd,
    ROUND(
        CASE WHEN AVG(price_usd) > 0
             THEN 100.0 * STDDEV(price_usd) / AVG(price_usd)
             ELSE NULL
        END::NUMERIC,
        2
    )                                           AS price_volatility_cov_pct
FROM v_calendar_enriched
WHERE price_usd IS NOT NULL
GROUP BY neighbourhood, room_type, DATE_TRUNC('month', date)
HAVING COUNT(price_usd) >= 15
ORDER BY neighbourhood, room_type, month;

-- ---------------------------------------------------------------------------
-- KPI 3: Guest Review Velocity (Stay Proxy)
-- Grain: Neighbourhood × Month
-- LIMITATION: Only 60–70% of guests write reviews; this is a lower-bound
-- proxy for completed stays, not a true booking count.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW kpi3_review_velocity AS
SELECT
    neighbourhood,
    DATE_TRUNC('month', date)::DATE             AS month,
    COUNT(id)                                   AS review_count,
    COUNT(DISTINCT listing_id)                  AS listings_with_reviews,
    ROUND(
        COUNT(id)::NUMERIC / NULLIF(COUNT(DISTINCT listing_id), 0),
        2
    )                                           AS avg_reviews_per_listing
FROM v_reviews_validated
GROUP BY neighbourhood, DATE_TRUNC('month', date)
ORDER BY neighbourhood, month;

-- ---------------------------------------------------------------------------
-- KPI 4: Weather Sensitivity Index
-- Grain: City-wide × Date
-- LIMITATION: Correlation ≠ causation. Tourism events, school calendars,
-- and local regulations are not controlled for in this analysis.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW kpi4_weather_sensitivity AS
SELECT * FROM v_weather_calendar_joined;

-- ---------------------------------------------------------------------------
-- KPI 5: Estimated Revenue Potential Proxy
-- Grain: Room Type × Neighbourhood × Month
-- LIMITATION: Assumes ALL unavailable days were booked at listed prices.
-- This is an upper-bound estimate only and will OVERESTIMATE true revenue.
-- Host blockouts and maintenance holds are included as if they were bookings.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS kpi5_revenue_proxy;
CREATE OR REPLACE VIEW kpi5_revenue_proxy AS
WITH proxy_pricing AS (
    SELECT
        listing_id,
        date,
        room_type,
        neighbourhood,
        CASE
            WHEN is_unavailable = TRUE AND price_usd IS NOT NULL THEN price_usd
            WHEN is_unavailable = TRUE AND price_usd IS NULL AND listing_price_usd IS NOT NULL THEN listing_price_usd
            ELSE NULL
        END AS proxy_price_usd,
        CASE
            WHEN is_unavailable = TRUE AND price_usd IS NOT NULL THEN 'calendar_price'
            WHEN is_unavailable = TRUE AND listing_price_usd IS NOT NULL THEN 'listing_price_fallback'
            ELSE 'unpriced'
        END AS price_basis,
        is_unavailable
    FROM v_calendar_enriched
)
SELECT
    room_type,
    neighbourhood,
    DATE_TRUNC('month', date)::DATE             AS month,
    COUNT(CASE WHEN is_unavailable THEN 1 END)  AS unavailable_days,
    ROUND(SUM(proxy_price_usd)::NUMERIC, 2)     AS rev_proxy_usd,
    COUNT(*) FILTER (WHERE price_basis = 'calendar_price') AS calendar_priced_unavailable_days,
    COUNT(*) FILTER (WHERE price_basis = 'listing_price_fallback') AS fallback_priced_unavailable_days,
    COUNT(*) FILTER (WHERE price_basis = 'unpriced') AS unpriced_unavailable_days,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE price_basis <> 'unpriced')
        / NULLIF(COUNT(*), 0), 2
    ) AS price_coverage_pct,
    CASE
        WHEN COUNT(*) FILTER (WHERE price_basis = 'unpriced') = COUNT(*) THEN 'unpriced'
        WHEN COUNT(*) FILTER (WHERE price_basis = 'unpriced') > 0 THEN 'mixed'
        WHEN COUNT(*) FILTER (WHERE price_basis = 'listing_price_fallback') > 0 THEN 'listing_price_fallback'
        ELSE 'calendar_price'
    END AS price_basis,
    COUNT(DISTINCT listing_id)                  AS listings_included
FROM proxy_pricing
WHERE is_unavailable = TRUE
GROUP BY room_type, neighbourhood, DATE_TRUNC('month', date)
ORDER BY room_type, neighbourhood, month;
