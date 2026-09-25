-- =============================================================================
-- 01_schema.sql — PostgreSQL DDL for FDE Ground Truth (Barcelona Airbnb)
-- =============================================================================
-- Idempotent: uses CREATE TABLE IF NOT EXISTS.
-- Staging tables (stg_*) receive cleaned data from Python transform stage.
-- Core tables (listings, calendar, etc.) are views or populated from staging.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- STAGING: Listings
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_listings (
    id                      BIGINT        PRIMARY KEY,
    host_id                 BIGINT,
    neighbourhood           TEXT,
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,
    room_type               TEXT,
    price_usd               NUMERIC(10, 2),
    last_scraped            DATE,
    number_of_reviews       INTEGER,
    review_scores_rating    NUMERIC(5, 2),
    minimum_nights          INTEGER
);

-- ---------------------------------------------------------------------------
-- STAGING: Calendar
-- Semantic constraint enforced: is_unavailable BOOLEAN (observed state).
-- There is NO is_booked or confirmed_booking field — this does not exist
-- in Inside Airbnb data and would be a fabrication.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_calendar (
    listing_id              BIGINT,
    date                    DATE,
    is_unavailable          BOOLEAN,          -- TRUE = 'f' in source; NOT a booking flag
    price_usd               NUMERIC(10, 2),
    adjusted_price_usd      NUMERIC(10, 2),
    minimum_nights          INTEGER,
    available               TEXT,             -- raw source value preserved
    PRIMARY KEY (listing_id, date)
);

-- ---------------------------------------------------------------------------
-- STAGING: Reviews
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_reviews (
    id                      BIGINT        PRIMARY KEY,
    listing_id              BIGINT,
    date                    DATE,
    reviewer_id             BIGINT,
    orphaned                BOOLEAN       DEFAULT FALSE
);

-- ---------------------------------------------------------------------------
-- STAGING: Weather Observations (from Open-Meteo API)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_weather (
    date                    DATE          PRIMARY KEY,
    temp_max_c              NUMERIC(6, 2),
    precipitation_mm        NUMERIC(8, 2),
    wind_speed_max_kmh      NUMERIC(8, 2),
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION
);

-- ---------------------------------------------------------------------------
-- STAGING: Neighbourhoods (reference)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_neighbourhoods (
    neighbourhood_group     TEXT,
    neighbourhood           TEXT,
    PRIMARY KEY (neighbourhood)
);

-- ---------------------------------------------------------------------------
-- INDEXES for join performance
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_stg_calendar_date        ON stg_calendar(date);
CREATE INDEX IF NOT EXISTS idx_stg_calendar_listing     ON stg_calendar(listing_id);
CREATE INDEX IF NOT EXISTS idx_stg_reviews_listing      ON stg_reviews(listing_id);
CREATE INDEX IF NOT EXISTS idx_stg_reviews_date         ON stg_reviews(date);
CREATE INDEX IF NOT EXISTS idx_stg_listings_neighbourhood ON stg_listings(neighbourhood);