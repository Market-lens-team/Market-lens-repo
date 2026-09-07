-- ============================================================
-- MARKETLENS GOLD LAYER - BUSINESS / SEMANTIC VIEWS
-- ============================================================
-- Project : market-lens-506611
-- Dataset : gold
--
-- These views sit on top of the Gold tables created by:
--
--     gold.sp_refresh_all()
--
-- The views do NOT store data.
-- They always read the latest Gold table data.
--
-- Gold tables used:
--   dim_security
--   dim_date
--   fact_daily_metrics
--   fact_period_returns
--   fact_drawdown_yearly
--   mart_screener
--   mart_sector_summary
--   mart_normalized_prices
--   mart_unusual_volume
-- ============================================================


-- ============================================================
-- 1. LATEST SNAPSHOT PER SYMBOL
-- ============================================================
-- Helper view.
-- Returns the most recent Gold daily metric for every symbol.
-- ============================================================

CREATE OR REPLACE VIEW
`market-lens-506611.gold.v_latest_snapshot_per_symbol`
AS
SELECT *
FROM
`market-lens-506611.gold.fact_daily_metrics`

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol
    ORDER BY Date DESC
) = 1;


-- ============================================================
-- 2. TRADING DAYS ONLY
-- ============================================================
-- Removes weekends/non-trading days from dim_date.
-- ============================================================

CREATE OR REPLACE VIEW
`market-lens-506611.gold.v_trading_days_only`
AS
SELECT *
FROM
`market-lens-506611.gold.dim_date`
WHERE is_trading_day = TRUE;


-- ============================================================
-- 3. TOP RETURNS
-- ============================================================
-- Useful for questions such as:
--
--   Which stocks had the highest 1-year return?
--   Which ETFs had the highest 3-month return?
-- ============================================================

CREATE OR REPLACE VIEW
`market-lens-506611.gold.v_top_returns`
AS
SELECT
    symbol,
    security_name,
    market_category,
    is_etf,
    return_1w,
    return_1m,
    return_3m,
    return_1y,
    return_3y

FROM
`market-lens-506611.gold.mart_screener`;


-- ============================================================
-- 4. UNUSUAL VOLUME
-- ============================================================
-- Uses the Gold volume_zscore.
--
-- > 2  = unusually high
-- < -2 = unusually low
-- otherwise normal
-- ============================================================

CREATE OR REPLACE VIEW
`market-lens-506611.gold.v_unusual_volume`
AS
SELECT
    symbol,
    Date,
    Volume,
    volume_zscore,

    CASE
        WHEN volume_zscore > 2
            THEN 'unusually high'

        WHEN volume_zscore < -2
            THEN 'unusually low'

        ELSE 'normal'
    END AS volume_flag

FROM
`market-lens-506611.gold.fact_daily_metrics`;


-- ============================================================
-- 5. SECTOR COMPARISON
-- ============================================================
-- Compares each security's 1-year return against the
-- average 1-year return of its market category.
--
-- IMPORTANT:
-- The Gold procedure creates:
--
--     mart_sector_summary
--
-- NOT:
--
--     mart_market_category_summary
-- ============================================================

CREATE OR REPLACE VIEW
`market-lens-506611.gold.v_sector_comparison`
AS
SELECT
    s.symbol,
    s.security_name,
    s.market_category,

    s.return_1y AS symbol_return_1y,

    summary.avg_market_category_return_1y,

    s.return_1y
        - summary.avg_market_category_return_1y
        AS vs_category_diff_pct

FROM
`market-lens-506611.gold.mart_screener` s

JOIN
`market-lens-506611.gold.mart_sector_summary` summary

ON s.market_category = summary.market_category;


-- ============================================================
-- 6. NORMALIZED PRICE COMPARISON
-- ============================================================
-- Used for comparing price performance across securities.
--
-- indexed_price starts from 100 for each symbol.
-- ============================================================

CREATE OR REPLACE VIEW
`market-lens-506611.gold.v_normalized_price_comparison`
AS
SELECT
    Date,
    symbol,
    Close,
    indexed_price,
    index_base_date

FROM
`market-lens-506611.gold.mart_normalized_prices`;


-- ============================================================
-- 7. DRAWDOWN BY YEAR
-- ============================================================
-- Useful for:
--
--   Which ETFs had the lowest drawdown in 2022?
--   Which stocks had the largest drawdown?
-- ============================================================

CREATE OR REPLACE VIEW
`market-lens-506611.gold.v_drawdown_by_year`
AS
SELECT
    d.symbol,

    ds.security_name,

    ds.market_category,

    ds.is_etf,

    d.year,

    d.max_drawdown_pct

FROM
`market-lens-506611.gold.fact_drawdown_yearly` d

JOIN
`market-lens-506611.gold.dim_security` ds

ON d.symbol = ds.symbol;


-- ============================================================
-- 8. SECURITY SCREENER
-- ============================================================
-- General-purpose business-facing view.
--
-- This exposes the complete screener information while
-- keeping consumers away from the physical Gold table.
-- ============================================================

CREATE OR REPLACE VIEW
`market-lens-506611.gold.v_security_screener`
AS
SELECT *
FROM
`market-lens-506611.gold.mart_screener`;