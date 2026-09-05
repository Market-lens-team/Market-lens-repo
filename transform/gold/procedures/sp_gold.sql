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
--
-- **CHANGED: trimmed to only what the problem statement asks for.**
-- The problem statement names exactly two example questions:
--   1. "which mid-cap stocks had unusual volume spikes last quarter?"
--   2. "compare AAPL against its sector peers over the last year"
-- Every object below is kept because it traces directly to one of
-- those two questions (or to general screening, which the problem
-- statement implies but doesn't name outright). Objects that don't
-- trace to either question were REMOVED, not just left unused:
--   dim_date, v_trading_days_only        - nothing needs a full
--                                           calendar dimension
--   fact_drawdown_yearly, v_drawdown_by_year - drawdown isn't asked
--                                           about in either question
--   v_unusual_volume                     - redundant with
--                                           mart_unusual_volume,
--                                           which already keeps only
--                                           the real outliers
--   v_security_screener,
--   v_latest_snapshot_per_symbol         - unfiltered passthroughs
--                                           with no distinct
--                                           consumer; mart_screener
--                                           already has this logic
--                                           inline
-- These are DROPPED explicitly (step 0.5, right after gold_base) so
-- redeploying this procedure also cleans up BigQuery, instead of
-- leaving nine stale, unmaintained objects sitting in the gold
-- dataset for someone to accidentally query later.
--
-- **CHANGED: incremental load.**
-- fact_daily_metrics used to be fully DROPPED and REBUILT from
-- gold_base's ENTIRE history on every single trigger, meaning years
-- of window-function recomputation on every incremental daily load.
-- It is now a MERGE bounded to a trailing per-symbol lookback window
-- (v_ma_lookback_days below), using a watermark against
-- fact_daily_metrics' own existing data.
--
-- **CHANGED: no sector/industry data in the source, and a**
-- **correlation-based alternative was tried and reverted.**
-- market_category (from symbols_valid_meta.csv) is a Nasdaq LISTING
-- TIER code (Q/G/S), not an industry sector - the source data has no
-- sector/industry field anywhere. A statistical alternative was built
-- (fact_symbol_correlation, v_symbol_peers - symbols whose daily
-- returns correlate over the last year, as a stand-in for "peers")
-- but it requires real trading history to produce a trustworthy
-- result, and fact_daily_metrics didn't have enough after being
-- rebuilt for the Volume type fix - it came back empty. Rather than
-- ship a dashboard panel with zero coverage for "compare AAPL to its
-- peers," mart_sector_summary / v_sector_comparison are kept as the
-- working fallback - just labeled honestly as "listing tier" wherever
-- they're surfaced, never as "sector." If real sector/industry data
-- becomes available later (an external reference table), that would
-- be the correct long-term fix; the correlation approach can also be
-- revisited once fact_daily_metrics has a full year of real history.
-- ============================================================

CREATE OR REPLACE PROCEDURE
`market-lens-506611.gold.sp_refresh_all`(p_run_id STRING)
-- p_run_id: passed in by the calling Cloud Function (Silver's marker
-- run_id), used purely to tag every audit row this run produces so
-- they can all be traced back to the same trigger later.
BEGIN

    -- v_step_started_at: reused before every step below to time-stamp
    -- that step's own audit row (reset via SET at the top of each step).
    DECLARE v_step_started_at TIMESTAMP;

    -- how many trailing calendar days fact_daily_metrics needs per
    -- symbol to correctly recompute moving_avg_200d (the longest
    -- window function in this table). 300 calendar days comfortably
    -- covers 200 trading days plus weekends/holidays.
    DECLARE v_ma_lookback_days INT64 DEFAULT 300;

    -- holds the row count actually affected by the fact_daily_metrics
    -- MERGE, so its audit row reflects what THIS run touched rather
    -- than the table's ever-growing total.
    DECLARE v_rows_affected INT64;

    -- ========================================================
    -- 0. Clean, deduplicated source data (symbol + Date)
    -- ========================================================
    -- Not individually audited - this is working data, not a
    -- Gold-layer deliverable table. Every later step reads from
    -- this temp table instead of hitting Silver directly.
    -- ========================================================

    -- gold_base: one row per symbol+Date, keeping only valid Silver
    -- rows and resolving duplicates by preferring the most recent
    -- write (silver_loaded_at is a real TIMESTAMP, unlike run_id
    -- which is a string that sorts alphabetically, not chronologically).
    CREATE TEMP TABLE gold_base AS
    SELECT *
    FROM `market-lens-506611.silver.silver_market_data`
    WHERE is_valid = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, Date
        ORDER BY silver_loaded_at DESC, Date DESC
    ) = 1;


    -- ========================================================
    -- 0.5. Clean up deprecated objects   **NEW**
    -- ========================================================
    -- Removes every object that doesn't trace to either
    -- problem-statement question (see the top-of-file note for the
    -- full reasoning per object). DROP ... IF EXISTS is safe to run
    -- every time - a no-op once these are already gone. Views are
    -- dropped before the tables they might reference, though
    -- BigQuery doesn't strictly require that ordering.
    -- Wrapped in its own BEGIN/EXCEPTION so that if a drop ever
    -- genuinely fails (e.g. a permissions issue), it's visible in the
    -- audit log without blocking every core object below from being
    -- rebuilt fresh.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        DROP VIEW IF EXISTS `market-lens-506611.gold.v_trading_days_only`;
        DROP VIEW IF EXISTS `market-lens-506611.gold.v_drawdown_by_year`;
        DROP VIEW IF EXISTS `market-lens-506611.gold.v_unusual_volume`;
        DROP VIEW IF EXISTS `market-lens-506611.gold.v_security_screener`;
        DROP VIEW IF EXISTS `market-lens-506611.gold.v_latest_snapshot_per_symbol`;

        DROP TABLE IF EXISTS `market-lens-506611.gold.dim_date`;
        DROP TABLE IF EXISTS `market-lens-506611.gold.fact_drawdown_yearly`;

        -- **CHANGED:** mart_sector_summary / v_sector_comparison are NO
        -- LONGER dropped - they're restored below (step 7) as an
        -- honestly-labeled "listing tier" comparison. The correlation-
        -- based peer approach (fact_symbol_correlation, v_symbol_peers)
        -- was removed instead: it requires real trading history to
        -- produce a trustworthy correlation, and fact_daily_metrics
        -- didn't have enough yet after being rebuilt for the Volume
        -- type fix. Rather than ship a dashboard panel with zero
        -- coverage for "compare AAPL to its peers," this reverts to
        -- the labeled-honestly listing-tier version, which has real
        -- data right now.

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_cleanup_deprecated_objects'), 'GOLD', 'cleanup_deprecated_objects', NULL,
            NULL, NULL, NULL, 7,
            'SUCCESS', v_step_started_at, CURRENT_TIMESTAMP(), NULL;

    EXCEPTION WHEN ERROR THEN

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_cleanup_deprecated_objects'), 'GOLD', 'cleanup_deprecated_objects', NULL,
            NULL, NULL, NULL, NULL,
            'FAILED', v_step_started_at, CURRENT_TIMESTAMP(),
            SUBSTR(@@error.message, 1, 5000);

        RAISE USING MESSAGE = @@error.message;

    END;


    -- ========================================================
    -- 1. dim_security
    -- ========================================================
    -- Security-level dimension table (one row per symbol) built
    -- straight from Silver's metadata table - name, exchange,
    -- category, and a derived is_etf boolean.
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
            ETF = 'Y' AS is_etf,          -- converts the Y/N flag into a proper boolean
            asset_type
        FROM `market-lens-506611.silver.silver_symbol_metadata`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY symbol
        ) = 1;                            -- safety net in case Silver's metadata ever has a duplicate symbol

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
    -- 2. fact_daily_metrics (monthly partitioned)
    -- ========================================================
    -- Table persists across runs; MERGEd using only a bounded
    -- trailing window per symbol (watermark = that symbol's latest
    -- Date already present, minus v_ma_lookback_days), instead of
    -- being dropped and rebuilt from full history every run.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE TABLE IF NOT EXISTS
        `market-lens-506611.gold.fact_daily_metrics`
        (
            Date DATE,
            symbol STRING,
            asset_type STRING,
            Close FLOAT64,
            Volume FLOAT64,   -- FLOAT64, not INT64 - matches Silver's actual declared type for this column
            daily_return_pct FLOAT64,
            rolling_avg_volume_30d FLOAT64,
            rolling_stddev_volume_30d FLOAT64,
            volume_zscore FLOAT64,
            rolling_max_close FLOAT64,
            drawdown_pct FLOAT64,
            moving_avg_20d FLOAT64,
            moving_avg_50d FLOAT64,
            moving_avg_200d FLOAT64,
            run_id STRING,
            gold_loaded_at TIMESTAMP
        )
        PARTITION BY DATE_TRUNC(Date, MONTH)
        CLUSTER BY symbol;

        MERGE `market-lens-506611.gold.fact_daily_metrics` T
        USING (

            WITH fact_watermarks AS (
                SELECT symbol, MAX(Date) AS last_fact_date
                FROM `market-lens-506611.gold.fact_daily_metrics`
                GROUP BY symbol
            ),

            recompute_scope AS (
                SELECT g.*
                FROM gold_base g
                LEFT JOIN fact_watermarks w ON w.symbol = g.symbol
                WHERE w.last_fact_date IS NULL
                   OR g.Date >= DATE_SUB(w.last_fact_date, INTERVAL v_ma_lookback_days DAY)
            )

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
            FROM recompute_scope

        ) S
        ON T.symbol = S.symbol AND T.Date = S.Date AND T.asset_type = S.asset_type

        WHEN MATCHED THEN
            UPDATE SET
                Close = S.Close,
                Volume = S.Volume,
                daily_return_pct = S.daily_return_pct,
                rolling_avg_volume_30d = S.rolling_avg_volume_30d,
                rolling_stddev_volume_30d = S.rolling_stddev_volume_30d,
                volume_zscore = S.volume_zscore,
                rolling_max_close = S.rolling_max_close,
                drawdown_pct = S.drawdown_pct,
                moving_avg_20d = S.moving_avg_20d,
                moving_avg_50d = S.moving_avg_50d,
                moving_avg_200d = S.moving_avg_200d,
                run_id = S.run_id,
                gold_loaded_at = S.gold_loaded_at

        WHEN NOT MATCHED THEN
            INSERT (
                Date, symbol, asset_type, Close, Volume, daily_return_pct,
                rolling_avg_volume_30d, rolling_stddev_volume_30d, volume_zscore,
                rolling_max_close, drawdown_pct,
                moving_avg_20d, moving_avg_50d, moving_avg_200d,
                run_id, gold_loaded_at
            )
            VALUES (
                S.Date, S.symbol, S.asset_type, S.Close, S.Volume, S.daily_return_pct,
                S.rolling_avg_volume_30d, S.rolling_stddev_volume_30d, S.volume_zscore,
                S.rolling_max_close, S.drawdown_pct,
                S.moving_avg_20d, S.moving_avg_50d, S.moving_avg_200d,
                S.run_id, S.gold_loaded_at
            );

        SET v_rows_affected = @@row_count;

        INSERT INTO `market-lens-506611.bronze.ingestion_audit`
        (batch_id, load_type, asset_type, gcs_path,
         expected_file_count, processed_file_count, expected_row_count,
         loaded_row_count, status, started_at, completed_at, error_message)
        SELECT
            CONCAT(p_run_id, '_fact_daily_metrics'), 'GOLD', 'fact_daily_metrics', NULL,
            NULL, NULL, NULL,
            v_rows_affected,
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
    -- 3. fact_period_returns
    -- ========================================================
    -- For every symbol, computes % return over 1W/1M/3M/1Y/3Y
    -- windows by matching the closest available trading date to
    -- each period's theoretical start date (handles gaps/holidays).
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
    -- 4. mart_screener
    -- ========================================================
    -- One row per symbol: its latest snapshot (price/volume/
    -- metrics) joined with security info and all period returns.
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
    -- 5. mart_normalized_prices (monthly partitioned)
    -- ========================================================
    -- Indexes each symbol's Close to 100 at its earliest available
    -- date. Full rebuild every run - indexing depends on each
    -- symbol's very first price, so it can't be bounded.
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
    -- 6. mart_unusual_volume
    -- ========================================================
    -- Latest row per symbol where |volume_zscore| > 2, i.e. today's
    -- volume is a statistical outlier vs its own 30-day average.
    -- Directly answers: "which stocks had unusual volume spikes?"
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

    -- v_top_returns: mart_screener narrowed to identity + return
    -- columns. Supports general screening (not a named problem-
    -- statement question, but implied by the overall goal).
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

    -- v_normalized_price_comparison: thin passthrough over
    -- mart_normalized_prices - exposes the indexed-to-100 price
    -- series, for visualizing a symbol's price path over time,
    -- optionally alongside others.
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


    -- ========================================================
    -- 7. mart_sector_summary   (restored - see step 0.5 note)
    -- ========================================================
    -- **NOTE:** despite the name, this aggregates by market_category,
    -- which is a Nasdaq LISTING TIER code (Q/G/S), NOT an industry
    -- sector - the source data (symbols_valid_meta.csv) has no
    -- sector/industry field at all. A correlation-based statistical
    -- peer group was tried instead (fact_symbol_correlation,
    -- v_symbol_peers) but required more trading history than
    -- fact_daily_metrics currently has, and came back empty. This is
    -- the honestly-labeled fallback: real data, clearly captioned as
    -- listing tier wherever it's shown on the dashboard, not sector.
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


    -- v_sector_comparison   (restored - see step 7 note above)
    -- Each symbol's own 1-year return next to its listing-tier
    -- category's average, plus the difference. Label this on the
    -- dashboard as "listing tier," never "sector."
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

END;