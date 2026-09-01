-- ============================================================
-- GOLD LAYER: STORED PROCEDURE
--
-- Rebuilds every dimension, fact, and mart table in Gold, in the
-- correct dependency order. Called via:
--     CALL market-lens-506611.gold.sp_refresh_all();
-- ============================================================
CREATE OR REPLACE PROCEDURE market-lens-506611.gold.sp_refresh_all()
BEGIN
    -- ========================================================
    -- 1. dim_security
    -- ========================================================
    CREATE OR REPLACE TABLE market-lens-506611.gold.dim_security
    CLUSTER BY symbol
    AS
    SELECT
        Symbol AS symbol,
        Security_Name AS security_name,
        Market_Category AS sector,
        Listing_Exchange AS exchange,
        ETF AS is_etf
    FROM market-lens-506611.bronze.bronze_symbol_metadata
    WHERE Test_Issue = 'N';
    -- ========================================================
    -- 2. dim_date
    -- ========================================================
    CREATE OR REPLACE TABLE market-lens-506611.gold.dim_date
    PARTITION BY Date
    AS
    WITH date_range AS (
        SELECT MIN(Date) AS min_date, MAX(Date) AS max_date
        FROM market-lens-506611.silver.silver_market_data
        WHERE is_valid = TRUE
    ),
    calendar AS (
        SELECT day AS Date
        FROM date_range, UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS day
    )
    SELECT
        c.Date,
        EXTRACT(YEAR FROM c.Date) AS year,
        EXTRACT(QUARTER FROM c.Date) AS quarter,
        EXTRACT(MONTH FROM c.Date) AS month,
        EXTRACT(DAYOFWEEK FROM c.Date) AS day_of_week,
        EXISTS (
            SELECT 1 FROM market-lens-506611.silver.silver_market_data f
            WHERE f.Date = c.Date AND f.is_valid = TRUE
        ) AS is_trading_day
    FROM calendar c;
    -- ========================================================
    -- 3. fact_daily_metrics
    -- ========================================================
    CREATE OR REPLACE TABLE market-lens-506611.gold.fact_daily_metrics
    PARTITION BY Date
    CLUSTER BY symbol
    AS
    SELECT
        Date,
        symbol,
        asset_type,
        Close,
        Volume,
        daily_return_pct,
        rolling_avg_volume_30d,
        rolling_stddev_volume_30d,
        SAFE_DIVIDE(Volume - rolling_avg_volume_30d, rolling_stddev_volume_30d) AS volume_zscore,
        rolling_max_close,
        SAFE_DIVIDE(Close - rolling_max_close, rolling_max_close) AS drawdown_pct,
        AVG(Close) OVER (
            PARTITION BY symbol ORDER BY Date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS moving_avg_20d,
        AVG(Close) OVER (
            PARTITION BY symbol ORDER BY Date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS moving_avg_50d,
        AVG(Close) OVER (
            PARTITION BY symbol ORDER BY Date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS moving_avg_200d
    FROM market-lens-506611.silver.silver_market_data
    WHERE is_valid = TRUE;
    -- ========================================================
    -- 4. fact_period_returns
    -- ========================================================
    CREATE OR REPLACE TABLE market-lens-506611.gold.fact_period_returns
    CLUSTER BY symbol
    AS
    WITH latest_date_per_symbol AS (
        SELECT symbol, MAX(Date) AS latest_date
        FROM market-lens-506611.silver.silver_market_data
        WHERE is_valid = TRUE
        GROUP BY symbol
    ),
    period_definitions AS (
        SELECT symbol, latest_date, '1W' AS period, DATE_SUB(latest_date, INTERVAL 1 WEEK) AS period_start_date
        FROM latest_date_per_symbol
        UNION ALL
        SELECT symbol, latest_date, '1M' AS period, DATE_SUB(latest_date, INTERVAL 1 MONTH) AS period_start_date
        FROM latest_date_per_symbol
        UNION ALL
        SELECT symbol, latest_date, '3M' AS period, DATE_SUB(latest_date, INTERVAL 3 MONTH) AS period_start_date
        FROM latest_date_per_symbol
        UNION ALL
        SELECT symbol, latest_date, '1Y' AS period, DATE_SUB(latest_date, INTERVAL 1 YEAR) AS period_start_date
        FROM latest_date_per_symbol
        UNION ALL
        SELECT symbol, latest_date, '3Y' AS period, DATE_SUB(latest_date, INTERVAL 3 YEAR) AS period_start_date
        FROM latest_date_per_symbol
    ),
    closest_start_prices AS (
        SELECT
            pd.symbol,
            pd.period,
            pd.latest_date,
            f.Date AS actual_start_date,
            f.Close AS start_close,
            ROW_NUMBER() OVER (
                PARTITION BY pd.symbol, pd.period
                ORDER BY ABS(DATE_DIFF(f.Date, pd.period_start_date, DAY))
            ) AS closeness_rank
        FROM period_definitions pd
        JOIN market-lens-506611.silver.silver_market_data f
            ON f.symbol = pd.symbol
            AND f.is_valid = TRUE
            AND f.Date BETWEEN DATE_SUB(pd.period_start_date, INTERVAL 7 DAY)
                            AND DATE_ADD(pd.period_start_date, INTERVAL 7 DAY)
    ),
    end_prices AS (
        SELECT f.symbol, f.Date AS end_date, f.Close AS end_close
        FROM market-lens-506611.silver.silver_market_data f
        JOIN latest_date_per_symbol l ON f.symbol = l.symbol AND f.Date = l.latest_date
        WHERE f.is_valid = TRUE
    )
    SELECT
        s.symbol,
        s.period,
        s.actual_start_date AS period_start_date,
        e.end_date AS period_end_date,
        s.start_close,
        e.end_close,
        SAFE_DIVIDE(e.end_close - s.start_close, s.start_close) * 100 AS return_pct
    FROM closest_start_prices s
    JOIN end_prices e ON s.symbol = e.symbol
    WHERE s.closeness_rank = 1;
    -- ========================================================
    -- 5. fact_drawdown_yearly
    -- ========================================================
    CREATE OR REPLACE TABLE market-lens-506611.gold.fact_drawdown_yearly
    CLUSTER BY symbol
    AS
    SELECT
        symbol,
        EXTRACT(YEAR FROM Date) AS year,
        MIN(drawdown_pct) AS max_drawdown_pct
    FROM market-lens-506611.gold.fact_daily_metrics
    WHERE drawdown_pct IS NOT NULL
    GROUP BY symbol, year;
    -- ========================================================
    -- 6. mart_screener
    -- ========================================================
    CREATE OR REPLACE TABLE market-lens-506611.gold.mart_screener
    CLUSTER BY symbol
    AS
    WITH latest_metrics AS (
        SELECT *
        FROM market-lens-506611.gold.fact_daily_metrics
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY Date DESC) = 1
    )
    SELECT
        lm.symbol,
        ds.security_name,
        ds.sector,
        ds.exchange,
        ds.is_etf,
        lm.Date AS as_of_date,
        lm.Close AS latest_close,
        lm.volume_zscore,
        lm.drawdown_pct,
        r1w.return_pct AS return_1w,
        r1m.return_pct AS return_1m,
        r3m.return_pct AS return_3m,
        r1y.return_pct AS return_1y,
        r3y.return_pct AS return_3y
    FROM latest_metrics lm
    LEFT JOIN market-lens-506611.gold.dim_security ds ON lm.symbol = ds.symbol
    LEFT JOIN market-lens-506611.gold.fact_period_returns r1w ON lm.symbol = r1w.symbol AND r1w.period = '1W'
    LEFT JOIN market-lens-506611.gold.fact_period_returns r1m ON lm.symbol = r1m.symbol AND r1m.period = '1M'
    LEFT JOIN market-lens-506611.gold.fact_period_returns r3m ON lm.symbol = r3m.symbol AND r3m.period = '3M'
    LEFT JOIN market-lens-506611.gold.fact_period_returns r1y ON lm.symbol = r1y.symbol AND r1y.period = '1Y'
    LEFT JOIN market-lens-506611.gold.fact_period_returns r3y ON lm.symbol = r3y.symbol AND r3y.period = '3Y';
    -- ========================================================
    -- 7. mart_sector_summary
    -- ========================================================
    CREATE OR REPLACE TABLE market-lens-506611.gold.mart_sector_summary
    AS
    SELECT
        sector,
        COUNT(*) AS symbol_count,
        AVG(return_1y) AS avg_sector_return_1y,
        AVG(return_1m) AS avg_sector_return_1m,
        STDDEV(return_1y) AS sector_volatility_1y
    FROM market-lens-506611.gold.mart_screener
    WHERE sector IS NOT NULL
    GROUP BY sector;
    -- ========================================================
    -- 8. mart_normalized_prices
    -- ========================================================
    CREATE OR REPLACE TABLE market-lens-506611.gold.mart_normalized_prices
    PARTITION BY Date
    CLUSTER BY symbol
    AS
    WITH base_prices AS (
        SELECT symbol, MIN(Date) AS base_date
        FROM market-lens-506611.silver.silver_market_data
        WHERE is_valid = TRUE
        GROUP BY symbol
    )
    SELECT
        f.Date,
        f.symbol,
        f.Close,
        b.base_date AS index_base_date,
        SAFE_DIVIDE(f.Close, base_close.Close) * 100 AS indexed_price
    FROM market-lens-506611.silver.silver_market_data f
    JOIN base_prices b ON f.symbol = b.symbol
    JOIN market-lens-506611.silver.silver_market_data base_close
        ON base_close.symbol = b.symbol AND base_close.Date = b.base_date
    WHERE f.is_valid = TRUE;
    -- ========================================================
    -- 9. mart_unusual_volume
    -- ========================================================
    CREATE OR REPLACE TABLE market-lens-506611.gold.mart_unusual_volume
    AS
    SELECT
        symbol,
        Date,
        Volume,
        volume_zscore
    FROM market-lens-506611.gold.fact_daily_metrics
    WHERE ABS(volume_zscore) > 2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY Date DESC) = 1;
END;