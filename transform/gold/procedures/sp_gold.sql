-- ============================================================
-- MARKETLENS GOLD LAYER
-- ============================================================
-- Source:
--   silver.silver_market_data
--   silver.silver_symbol_metadata
--
-- Procedure:
--   gold.sp_refresh_all(p_run_id STRING)
--
-- p_run_id is used ONLY to correlate this run's audit rows back
-- to the triggering Silver run (batch_id = <p_run_id>_<step>).
-- It is NOT used in any table's data/business logic.
--
-- Gold trigger:
--   gold_trigger/<run_id>/_READY
--
-- Partitioning:
--   Large date-based tables use MONTHLY partitioning.
--
-- Duplicate key:
--   symbol + Date  (handled once via gold_base at the top)
--
-- Audit:
--   Every table AND every view writes its own SUCCESS/FAILED row
--   to bronze.ingestion_audit, mirroring Bronze's per-batch audit
--   pattern. A failure in one step still fails the whole
--   procedure (RAISE after logging), so the Cloud Function's own
--   overall audit row still fires too.
-- ============================================================

CREATE OR REPLACE PROCEDURE
`market-lens-506611.gold.sp_refresh_all`(p_run_id STRING)
BEGIN

    DECLARE v_step_started_at TIMESTAMP;

    -- ========================================================
    -- 0. Clean, deduplicated source data (symbol + Date)
    -- ========================================================
    -- Not individually audited - this is working data, not a
    -- Gold-layer deliverable table.
    -- ========================================================

    CREATE TEMP TABLE gold_base AS
    SELECT *
    FROM `market-lens-506611.silver.silver_market_data`
    WHERE is_valid = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, Date
        ORDER BY run_id DESC, Date DESC
    ) = 1;


    -- ========================================================
    -- 1. dim_security
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.dim_security`
        CLUSTER BY symbol
        AS
        SELECT
            symbol,
            security_name,
            listing_exchange AS exchange,
            market_category,
            ETF = 'Y' AS is_etf,
            asset_type
        FROM `market-lens-506611.silver.silver_symbol_metadata`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY symbol
        ) = 1;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_dim_security'), 'GOLD', 'dim_security', NULL,
            NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.dim_security`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_dim_security'), 'GOLD', 'dim_security', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- 2. dim_date
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.dim_date`
        AS
        WITH date_range AS (
            SELECT MIN(Date) AS min_date, MAX(Date) AS max_date
            FROM gold_base
        ),
        calendar AS (
            SELECT day AS Date
            FROM date_range,
            UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS day
        ),
        trading_dates AS (
            SELECT DISTINCT Date FROM gold_base
        )
        SELECT
            c.Date,
            EXTRACT(YEAR FROM c.Date) AS year,
            EXTRACT(QUARTER FROM c.Date) AS quarter,
            EXTRACT(MONTH FROM c.Date) AS month,
            EXTRACT(DAYOFWEEK FROM c.Date) AS day_of_week,
            t.Date IS NOT NULL AS is_trading_day
        FROM calendar c
        LEFT JOIN trading_dates t ON t.Date = c.Date;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_dim_date'), 'GOLD', 'dim_date', NULL,
            NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.dim_date`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_dim_date'), 'GOLD', 'dim_date', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- 3. fact_daily_metrics (monthly partitioned)
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        DROP TABLE IF EXISTS
        `market-lens-506611.gold.fact_daily_metrics`;

        CREATE TABLE
        `market-lens-506611.gold.fact_daily_metrics`
        PARTITION BY DATE_TRUNC(Date, MONTH)
        CLUSTER BY symbol
        AS
        SELECT
            Date, symbol, asset_type, Close, Volume, daily_return_pct,
            rolling_avg_volume_30d, rolling_stddev_volume_30d,
            SAFE_DIVIDE(Volume - rolling_avg_volume_30d, rolling_stddev_volume_30d) AS volume_zscore,
            rolling_max_close,
            SAFE_DIVIDE(Close - rolling_max_close, rolling_max_close) AS drawdown_pct,
            AVG(Close) OVER (PARTITION BY symbol ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS moving_avg_20d,
            AVG(Close) OVER (PARTITION BY symbol ORDER BY Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS moving_avg_50d,
            AVG(Close) OVER (PARTITION BY symbol ORDER BY Date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS moving_avg_200d,
            run_id,
            CURRENT_TIMESTAMP() AS gold_loaded_at
        FROM gold_base;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_fact_daily_metrics'), 'GOLD', 'fact_daily_metrics', NULL,
            NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.fact_daily_metrics`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_fact_daily_metrics'), 'GOLD', 'fact_daily_metrics', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- 4. fact_period_returns
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.fact_period_returns`
        CLUSTER BY symbol
        AS
        WITH latest_date_per_symbol AS (
            SELECT symbol, MAX(Date) AS latest_date
            FROM gold_base
            GROUP BY symbol
        ),
        period_definitions AS (
            SELECT symbol, latest_date, '1W' AS period, DATE_SUB(latest_date, INTERVAL 1 WEEK) AS period_start_date FROM latest_date_per_symbol
            UNION ALL
            SELECT symbol, latest_date, '1M', DATE_SUB(latest_date, INTERVAL 1 MONTH) FROM latest_date_per_symbol
            UNION ALL
            SELECT symbol, latest_date, '3M', DATE_SUB(latest_date, INTERVAL 3 MONTH) FROM latest_date_per_symbol
            UNION ALL
            SELECT symbol, latest_date, '1Y', DATE_SUB(latest_date, INTERVAL 1 YEAR) FROM latest_date_per_symbol
            UNION ALL
            SELECT symbol, latest_date, '3Y', DATE_SUB(latest_date, INTERVAL 3 YEAR) FROM latest_date_per_symbol
        ),
        closest_start_prices AS (
            SELECT
                pd.symbol, pd.period, pd.latest_date,
                f.Date AS actual_start_date, f.Close AS start_close,
                ROW_NUMBER() OVER (
                    PARTITION BY pd.symbol, pd.period
                    ORDER BY ABS(DATE_DIFF(f.Date, pd.period_start_date, DAY)), f.Date DESC
                ) AS closeness_rank
            FROM period_definitions pd
            JOIN gold_base f
                ON f.symbol = pd.symbol
                AND f.Date BETWEEN DATE_SUB(pd.period_start_date, INTERVAL 7 DAY) AND DATE_ADD(pd.period_start_date, INTERVAL 7 DAY)
        ),
        end_prices AS (
            SELECT f.symbol, f.Date AS end_date, f.Close AS end_close
            FROM gold_base f
            JOIN latest_date_per_symbol l ON f.symbol = l.symbol AND f.Date = l.latest_date
        )
        SELECT
            s.symbol, s.period, s.actual_start_date AS period_start_date, e.end_date AS period_end_date,
            s.start_close, e.end_close,
            SAFE_DIVIDE(e.end_close - s.start_close, s.start_close) * 100 AS return_pct
        FROM closest_start_prices s
        JOIN end_prices e ON s.symbol = e.symbol
        WHERE s.closeness_rank = 1;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_fact_period_returns'), 'GOLD', 'fact_period_returns', NULL,
            NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.fact_period_returns`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_fact_period_returns'), 'GOLD', 'fact_period_returns', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- 5. fact_drawdown_yearly
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.fact_drawdown_yearly`
        CLUSTER BY symbol
        AS
        SELECT
            symbol, EXTRACT(YEAR FROM Date) AS year, MIN(drawdown_pct) AS max_drawdown_pct
        FROM `market-lens-506611.gold.fact_daily_metrics`
        WHERE drawdown_pct IS NOT NULL
        GROUP BY symbol, year;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_fact_drawdown_yearly'), 'GOLD', 'fact_drawdown_yearly', NULL,
            NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.fact_drawdown_yearly`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_fact_drawdown_yearly'), 'GOLD', 'fact_drawdown_yearly', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- 6. mart_screener
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.mart_screener`
        CLUSTER BY symbol
        AS
        WITH latest_metrics AS (
            SELECT *
            FROM `market-lens-506611.gold.fact_daily_metrics`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY Date DESC) = 1
        )
        SELECT
            lm.symbol, ds.security_name, ds.market_category, ds.exchange, ds.is_etf,
            lm.asset_type, lm.Date AS as_of_date, lm.Close AS latest_close, lm.Volume AS latest_volume,
            lm.volume_zscore, lm.drawdown_pct,
            lm.moving_avg_20d, lm.moving_avg_50d, lm.moving_avg_200d,
            r1w.return_pct AS return_1w, r1m.return_pct AS return_1m, r3m.return_pct AS return_3m,
            r1y.return_pct AS return_1y, r3y.return_pct AS return_3y,
            lm.run_id, lm.gold_loaded_at
        FROM latest_metrics lm
        LEFT JOIN `market-lens-506611.gold.dim_security` ds ON lm.symbol = ds.symbol
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r1w ON lm.symbol = r1w.symbol AND r1w.period = '1W'
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r1m ON lm.symbol = r1m.symbol AND r1m.period = '1M'
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r3m ON lm.symbol = r3m.symbol AND r3m.period = '3M'
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r1y ON lm.symbol = r1y.symbol AND r1y.period = '1Y'
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r3y ON lm.symbol = r3y.symbol AND r3y.period = '3Y';

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_mart_screener'), 'GOLD', 'mart_screener', NULL,
            NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.mart_screener`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_mart_screener'), 'GOLD', 'mart_screener', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- 7. mart_sector_summary
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.mart_sector_summary`
        AS
        SELECT
            market_category,
            COUNT(*) AS symbol_count,
            AVG(return_1y) AS avg_market_category_return_1y,
            AVG(return_1m) AS avg_market_category_return_1m,
            STDDEV(return_1y) AS market_category_volatility_1y
        FROM `market-lens-506611.gold.mart_screener`
        WHERE market_category IS NOT NULL
        GROUP BY market_category;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_mart_sector_summary'), 'GOLD', 'mart_sector_summary', NULL,
            NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.mart_sector_summary`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_mart_sector_summary'), 'GOLD', 'mart_sector_summary', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- 8. mart_normalized_prices (monthly partitioned)
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        DROP TABLE IF EXISTS
        `market-lens-506611.gold.mart_normalized_prices`;

        CREATE TABLE
        `market-lens-506611.gold.mart_normalized_prices`
        PARTITION BY DATE_TRUNC(Date, MONTH)
        CLUSTER BY symbol
        AS
        WITH base_prices AS (
            SELECT symbol, MIN(Date) AS base_date
            FROM gold_base
            GROUP BY symbol
        )
        SELECT
            f.Date, f.symbol, f.Close, b.base_date AS index_base_date,
            SAFE_DIVIDE(f.Close, base_close.Close) * 100 AS indexed_price
        FROM gold_base f
        JOIN base_prices b ON f.symbol = b.symbol
        JOIN gold_base base_close ON base_close.symbol = b.symbol AND base_close.Date = b.base_date;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_mart_normalized_prices'), 'GOLD', 'mart_normalized_prices', NULL,
            NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.mart_normalized_prices`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_mart_normalized_prices'), 'GOLD', 'mart_normalized_prices', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- 9. mart_unusual_volume
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.mart_unusual_volume`
        AS
        SELECT
            symbol, Date, Volume, volume_zscore, asset_type, run_id, gold_loaded_at
        FROM `market-lens-506611.gold.fact_daily_metrics`
        WHERE ABS(volume_zscore) > 2
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY Date DESC) = 1;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_mart_unusual_volume'), 'GOLD', 'mart_unusual_volume', NULL,
            NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.mart_unusual_volume`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_mart_unusual_volume'), 'GOLD', 'mart_unusual_volume', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- VIEWS (recreated + audited every Gold run)
    -- ========================================================

    -- v_latest_snapshot_per_symbol
    SET v_step_started_at = CURRENT_TIMESTAMP();
    BEGIN
        CREATE OR REPLACE VIEW `market-lens-506611.gold.v_latest_snapshot_per_symbol` AS
        SELECT * FROM `market-lens-506611.gold.fact_daily_metrics`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY Date DESC) = 1;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_latest_snapshot_per_symbol'), 'GOLD', 'v_latest_snapshot_per_symbol',
            NULL, NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.v_latest_snapshot_per_symbol`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;
    EXCEPTION WHEN ERROR THEN
        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_latest_snapshot_per_symbol'), 'GOLD', 'v_latest_snapshot_per_symbol',
            NULL, NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(), SUBSTR(@@error.message, 1, 5000);
        RAISE USING MESSAGE = @@error.message;
    END;

    -- v_trading_days_only
    SET v_step_started_at = CURRENT_TIMESTAMP();
    BEGIN
        CREATE OR REPLACE VIEW `market-lens-506611.gold.v_trading_days_only` AS
        SELECT * FROM `market-lens-506611.gold.dim_date` WHERE is_trading_day = TRUE;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_trading_days_only'), 'GOLD', 'v_trading_days_only',
            NULL, NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.v_trading_days_only`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;
    EXCEPTION WHEN ERROR THEN
        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_trading_days_only'), 'GOLD', 'v_trading_days_only',
            NULL, NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(), SUBSTR(@@error.message, 1, 5000);
        RAISE USING MESSAGE = @@error.message;
    END;

    -- v_top_returns
    SET v_step_started_at = CURRENT_TIMESTAMP();
    BEGIN
        CREATE OR REPLACE VIEW `market-lens-506611.gold.v_top_returns` AS
        SELECT symbol, security_name, market_category, is_etf,
               return_1w, return_1m, return_3m, return_1y, return_3y
        FROM `market-lens-506611.gold.mart_screener`;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_top_returns'), 'GOLD', 'v_top_returns',
            NULL, NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.v_top_returns`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;
    EXCEPTION WHEN ERROR THEN
        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_top_returns'), 'GOLD', 'v_top_returns',
            NULL, NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(), SUBSTR(@@error.message, 1, 5000);
        RAISE USING MESSAGE = @@error.message;
    END;

    -- v_unusual_volume
    SET v_step_started_at = CURRENT_TIMESTAMP();
    BEGIN
        CREATE OR REPLACE VIEW `market-lens-506611.gold.v_unusual_volume` AS
        SELECT symbol, Date, Volume, volume_zscore,
            CASE
                WHEN volume_zscore > 2 THEN 'unusually high'
                WHEN volume_zscore < -2 THEN 'unusually low'
                ELSE 'normal'
            END AS volume_flag
        FROM `market-lens-506611.gold.fact_daily_metrics`;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_unusual_volume'), 'GOLD', 'v_unusual_volume',
            NULL, NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.v_unusual_volume`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;
    EXCEPTION WHEN ERROR THEN
        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_unusual_volume'), 'GOLD', 'v_unusual_volume',
            NULL, NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(), SUBSTR(@@error.message, 1, 5000);
        RAISE USING MESSAGE = @@error.message;
    END;

    -- v_sector_comparison
    SET v_step_started_at = CURRENT_TIMESTAMP();
    BEGIN
        CREATE OR REPLACE VIEW `market-lens-506611.gold.v_sector_comparison` AS
        SELECT
            s.symbol, s.security_name, s.market_category,
            s.return_1y AS symbol_return_1y,
            summary.avg_market_category_return_1y,
            s.return_1y - summary.avg_market_category_return_1y AS vs_category_diff_pct
        FROM `market-lens-506611.gold.mart_screener` s
        JOIN `market-lens-506611.gold.mart_sector_summary` summary
            ON s.market_category = summary.market_category;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_sector_comparison'), 'GOLD', 'v_sector_comparison',
            NULL, NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.v_sector_comparison`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;
    EXCEPTION WHEN ERROR THEN
        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_sector_comparison'), 'GOLD', 'v_sector_comparison',
            NULL, NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(), SUBSTR(@@error.message, 1, 5000);
        RAISE USING MESSAGE = @@error.message;
    END;

    -- v_normalized_price_comparison
    SET v_step_started_at = CURRENT_TIMESTAMP();
    BEGIN
        CREATE OR REPLACE VIEW `market-lens-506611.gold.v_normalized_price_comparison` AS
        SELECT Date, symbol, Close, indexed_price, index_base_date
        FROM `market-lens-506611.gold.mart_normalized_prices`;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_normalized_price_comparison'), 'GOLD', 'v_normalized_price_comparison',
            NULL, NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.v_normalized_price_comparison`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;
    EXCEPTION WHEN ERROR THEN
        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_normalized_price_comparison'), 'GOLD', 'v_normalized_price_comparison',
            NULL, NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(), SUBSTR(@@error.message, 1, 5000);
        RAISE USING MESSAGE = @@error.message;
    END;

    -- v_drawdown_by_year
    SET v_step_started_at = CURRENT_TIMESTAMP();
    BEGIN
        CREATE OR REPLACE VIEW `market-lens-506611.gold.v_drawdown_by_year` AS
        SELECT d.symbol, ds.security_name, ds.market_category, ds.is_etf, d.year, d.max_drawdown_pct
        FROM `market-lens-506611.gold.fact_drawdown_yearly` d
        JOIN `market-lens-506611.gold.dim_security` ds ON d.symbol = ds.symbol;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_drawdown_by_year'), 'GOLD', 'v_drawdown_by_year',
            NULL, NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.v_drawdown_by_year`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;
    EXCEPTION WHEN ERROR THEN
        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_drawdown_by_year'), 'GOLD', 'v_drawdown_by_year',
            NULL, NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(), SUBSTR(@@error.message, 1, 5000);
        RAISE USING MESSAGE = @@error.message;
    END;

    -- v_security_screener
    SET v_step_started_at = CURRENT_TIMESTAMP();
    BEGIN
        CREATE OR REPLACE VIEW `market-lens-506611.gold.v_security_screener` AS
        SELECT * FROM `market-lens-506611.gold.mart_screener`;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_security_screener'), 'GOLD', 'v_security_screener',
            NULL, NULL, NULL, NULL,
            (SELECT COUNT(*) FROM `market-lens-506611.gold.v_security_screener`),
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;
    EXCEPTION WHEN ERROR THEN
        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path, expected_file_count, processed_file_count,
         expected_row_count, loaded_row_count, status, started_at, completed_at, error_message)
        SELECT CONCAT(p_run_id, '_v_security_screener'), 'GOLD', 'v_security_screener',
            NULL, NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(), SUBSTR(@@error.message, 1, 5000);
        RAISE USING MESSAGE = @@error.message;
    END;

END;