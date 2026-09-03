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
-- **CHANGED: incremental load.**
-- fact_daily_metrics (step 3) used to be fully DROPPED and REBUILT
-- from gold_base's ENTIRE history on every single trigger, meaning
-- years of window-function recomputation on every incremental daily
-- load. It is now a MERGE bounded to a trailing per-symbol lookback
-- window (v_ma_lookback_days below), using a watermark against
-- fact_daily_metrics' own existing data. Everything else in this
-- procedure is left as a full rebuild ON PURPOSE - they're cheap
-- aggregations/dimensions downstream of fact_daily_metrics, not the
-- cost driver.
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

    -- **CHANGED:** how many trailing calendar days fact_daily_metrics
    -- needs per symbol to correctly recompute moving_avg_200d (the
    -- longest window function in this table). 300 calendar days
    -- comfortably covers 200 trading days plus weekends/holidays.
    DECLARE v_ma_lookback_days INT64 DEFAULT 300;

    -- **CHANGED:** holds the row count actually affected by the
    -- fact_daily_metrics MERGE (step 3), so its audit row reflects
    -- what THIS run touched rather than the table's ever-growing total.
    DECLARE v_rows_affected INT64;

    -- ========================================================
    -- 0. Clean, deduplicated source data (symbol + Date)
    -- ========================================================
    -- Not individually audited - this is working data, not a
    -- Gold-layer deliverable table. Every later step reads from
    -- this temp table instead of hitting Silver directly.
    -- ========================================================

    -- gold_base: one row per symbol+Date, keeping only valid Silver
    -- rows and resolving duplicates by preferring the most recent run_id.
    -- **CHANGED (Problem 2 fix):** was ORDER BY run_id DESC, Date DESC.
    -- run_id is a STRING like 'historical_20260101120000' or
    -- 'incremental_20260315090000' - sorting it alphabetically means
    -- any 'incremental_...' row ALWAYS outranks any 'historical_...'
    -- row, regardless of which one is actually more recent, because
    -- 'i' > 'h' as characters. A historical backfill run TODAY could
    -- silently lose to an incremental run from months ago.
    -- silver_loaded_at is a real TIMESTAMP (set via CURRENT_TIMESTAMP()
    -- when Silver last wrote this row), so ordering by it directly
    -- reflects true recency regardless of load type.
    CREATE TEMP TABLE gold_base AS
    SELECT *
    FROM `market-lens-506611.silver.silver_market_data`
    WHERE is_valid = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, Date
        ORDER BY silver_loaded_at DESC, Date DESC
    ) = 1;


    -- ========================================================
    -- 1. dim_security
    -- ========================================================
    -- Security-level dimension table (one row per symbol) built
    -- straight from Silver's metadata table - name, exchange,
    -- category, and a derived is_etf boolean.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        -- Full rebuild every run - cheap, since this table is
        -- one row per symbol, not per symbol+Date.
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

        -- Success audit row: records this step's row count and timing.
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

        -- Failure audit row, then re-raise so the whole procedure
        -- (and the calling Cloud Function) also fails loudly.
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
    -- Calendar dimension spanning every date in gold_base's range
    -- (not just trading days), flagging which ones are actual
    -- trading days. Needed for gap-aware date-range queries.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        -- Full rebuild every run - this table genuinely needs the
        -- complete min-to-max date range, so bounding it would be wrong.
        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.dim_date`
        AS
        WITH date_range AS (
            SELECT MIN(Date) AS min_date, MAX(Date) AS max_date
            FROM gold_base
        ),
        calendar AS (
            -- generates EVERY calendar day between min and max, trading or not
            SELECT day AS Date
            FROM date_range,
            UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS day
        ),
        trading_dates AS (
            SELECT DISTINCT Date FROM gold_base   -- the actual dates that have real trading data
        )
        SELECT
            c.Date,
            EXTRACT(YEAR FROM c.Date) AS year,
            EXTRACT(QUARTER FROM c.Date) AS quarter,
            EXTRACT(MONTH FROM c.Date) AS month,
            EXTRACT(DAYOFWEEK FROM c.Date) AS day_of_week,
            t.Date IS NOT NULL AS is_trading_day   -- TRUE only if this calendar day appears in trading_dates
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
    -- **CHANGED FOR INCREMENTAL LOAD**
    --
    -- Was: DROP TABLE IF EXISTS + CREATE TABLE AS SELECT from
    -- gold_base's ENTIRE history, every single run.
    --
    -- Now: table is created once (if it doesn't already exist) with
    -- an explicit schema, then MERGEd using only a bounded trailing
    -- window per symbol (watermark = that symbol's latest Date
    -- already present in fact_daily_metrics, minus v_ma_lookback_days).
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        -- **CHANGED:** table must persist across runs for MERGE to
        -- target it - this replaces the old DROP + CREATE AS SELECT,
        -- which always started from an empty table on every run.
        -- IF NOT EXISTS makes this a one-time setup, safe to re-run.
        CREATE TABLE IF NOT EXISTS
        `market-lens-506611.gold.fact_daily_metrics`
        (
            Date DATE,
            symbol STRING,
            asset_type STRING,
            Close FLOAT64,
            -- CORRECTED: was INT64 - Bronze casts Volume to INT64 before
            -- loading it, but Silver's own declared schema (silver_market_data_schema.json)
            -- types Volume as FLOAT64, and that is what actually flows into
            -- gold_base -> recompute_scope -> this MERGE. INT64 here caused
            -- 'Value of type FLOAT64 cannot be assigned to Volume, which has
            -- type INT64' at MERGE time. FLOAT64 matches what Silver actually sends.
            Volume FLOAT64,
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
        PARTITION BY DATE_TRUNC(Date, MONTH)   -- monthly partitions keep date-range queries and partition pruning cheap
        CLUSTER BY symbol;                     -- clustering by symbol speeds up per-symbol lookups within a partition

        -- **CHANGED:** MERGE instead of a full CREATE TABLE AS SELECT -
        -- this is the actual incremental-load mechanism for this table.
        MERGE `market-lens-506611.gold.fact_daily_metrics` T
        USING (

            -- **CHANGED:** per-symbol watermark against
            -- fact_daily_metrics itself (not gold_base) - tells us how
            -- far THIS table has already been computed for each symbol.
            WITH fact_watermarks AS (
                SELECT symbol, MAX(Date) AS last_fact_date
                FROM `market-lens-506611.gold.fact_daily_metrics`
                GROUP BY symbol
            ),

            -- **CHANGED:** only the trailing window each symbol needs
            -- to correctly recompute its newest moving averages -
            -- new symbols (no watermark) get their full history once.
            recompute_scope AS (
                SELECT g.*
                FROM gold_base g
                LEFT JOIN fact_watermarks w ON w.symbol = g.symbol
                WHERE w.last_fact_date IS NULL
                   OR g.Date >= DATE_SUB(w.last_fact_date, INTERVAL v_ma_lookback_days DAY)
            )

            -- computes every derived metric this table exposes, scoped
            -- to just recompute_scope instead of all of gold_base
            SELECT
                Date, symbol, asset_type, Close, Volume, daily_return_pct,
                rolling_avg_volume_30d, rolling_stddev_volume_30d,
                SAFE_DIVIDE(Volume - rolling_avg_volume_30d, rolling_stddev_volume_30d) AS volume_zscore,   -- how many std-devs today's volume is from its 30-day average
                rolling_max_close,
                SAFE_DIVIDE(Close - rolling_max_close, rolling_max_close) AS drawdown_pct,   -- % below the all-time high, negative or zero
                AVG(Close) OVER (PARTITION BY symbol ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS moving_avg_20d,
                AVG(Close) OVER (PARTITION BY symbol ORDER BY Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS moving_avg_50d,
                AVG(Close) OVER (PARTITION BY symbol ORDER BY Date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS moving_avg_200d,
                run_id,
                CURRENT_TIMESTAMP() AS gold_loaded_at
            FROM recompute_scope

        ) S
        ON T.symbol = S.symbol AND T.Date = S.Date AND T.asset_type = S.asset_type

        WHEN MATCHED THEN
            -- row already exists (e.g. inside the re-scanned buffer window) -> overwrite with freshly recomputed values
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
            -- genuinely new symbol+Date row -> insert it
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

        -- **CHANGED:** @@row_count captures the MERGE's own affected-row
        -- count, so the audit row reflects what THIS run touched -
        -- a full COUNT(*) here would just report the ever-growing total.
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
    -- 4. fact_period_returns
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
            -- the most recent trading date on record for each symbol - the "as of" anchor for every return calc
            SELECT symbol, MAX(Date) AS latest_date
            FROM gold_base
            GROUP BY symbol
        ),
        period_definitions AS (
            -- one row per symbol per period, giving the theoretical start date to look up a price near
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
            -- for each symbol+period, finds the actual trading day closest to the theoretical start date (within +/- 7 days)
            SELECT
                pd.symbol, pd.period, pd.latest_date,
                f.Date AS actual_start_date, f.Close AS start_close,
                ROW_NUMBER() OVER (
                    PARTITION BY pd.symbol, pd.period
                    ORDER BY ABS(DATE_DIFF(f.Date, pd.period_start_date, DAY)), f.Date DESC
                ) AS closeness_rank   -- rank 1 = closest match to the theoretical start date
            FROM period_definitions pd
            JOIN gold_base f
                ON f.symbol = pd.symbol
                AND f.Date BETWEEN DATE_SUB(pd.period_start_date, INTERVAL 7 DAY) AND DATE_ADD(pd.period_start_date, INTERVAL 7 DAY)
        ),
        end_prices AS (
            -- the actual latest-date Close price per symbol, used as the return's end point
            SELECT f.symbol, f.Date AS end_date, f.Close AS end_close
            FROM gold_base f
            JOIN latest_date_per_symbol l ON f.symbol = l.symbol AND f.Date = l.latest_date
        )
        SELECT
            s.symbol, s.period, s.actual_start_date AS period_start_date, e.end_date AS period_end_date,
            s.start_close, e.end_close,
            SAFE_DIVIDE(e.end_close - s.start_close, s.start_close) * 100 AS return_pct   -- % return over the period
        FROM closest_start_prices s
        JOIN end_prices e ON s.symbol = e.symbol
        WHERE s.closeness_rank = 1;   -- keep only the single closest-matched start price per symbol+period

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
    -- Worst (most negative) drawdown_pct per symbol per calendar
    -- year, aggregated from fact_daily_metrics. Answers "biggest
    -- drop from a high, by year" questions.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.fact_drawdown_yearly`
        CLUSTER BY symbol
        AS
        SELECT
            symbol, EXTRACT(YEAR FROM Date) AS year, MIN(drawdown_pct) AS max_drawdown_pct   -- MIN because drawdown_pct is negative; the most negative = the worst drop
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
    -- One row per symbol: its latest snapshot (price/volume/
    -- metrics) joined with security info and all period returns.
    -- The main table behind stock-screener-style questions.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.mart_screener`
        CLUSTER BY symbol
        AS
        WITH latest_metrics AS (
            -- keeps only each symbol's single most recent row from fact_daily_metrics
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
        -- five separate self-joins against fact_period_returns, one per period, to pivot rows into columns
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
    -- Aggregates mart_screener up to one row per market_category
    -- (avg return, volatility, symbol count) - powers sector-vs-
    -- sector comparison questions.
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
            STDDEV(return_1y) AS market_category_volatility_1y   -- spread of 1-year returns within the category, as a volatility proxy
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
    -- Indexes each symbol's Close to 100 at its earliest available
    -- date, so different symbols' price paths become visually and
    -- numerically comparable regardless of absolute price level.
    -- NOT changed to incremental - still a full rebuild every run,
    -- since indexing depends on each symbol's very first price.
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
            -- each symbol's earliest available trading date - the "100" anchor point
            SELECT symbol, MIN(Date) AS base_date
            FROM gold_base
            GROUP BY symbol
        )
        SELECT
            f.Date, f.symbol, f.Close, b.base_date AS index_base_date,
            SAFE_DIVIDE(f.Close, base_close.Close) * 100 AS indexed_price   -- every price expressed as a % of the base-date price
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
    -- Latest row per symbol where |volume_zscore| > 2, i.e. today's
    -- volume is a statistical outlier vs its own 30-day average.
    -- Powers "which stocks had unusual volume" questions.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.mart_unusual_volume`
        AS
        SELECT
            symbol, Date, Volume, volume_zscore, asset_type, run_id, gold_loaded_at
        FROM `market-lens-506611.gold.fact_daily_metrics`
        WHERE ABS(volume_zscore) > 2   -- more than 2 standard deviations from the 30-day average volume
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
    -- Each view is a thin, always-fresh read layer on top of the
    -- tables above - no data stored, just a saved query. Recreated
    -- every run so their definitions stay in sync with this
    -- procedure, and audited the same way as the tables.
    -- ========================================================

    -- v_latest_snapshot_per_symbol: same as mart_screener's inner
    -- CTE, exposed as its own reusable view - every symbol's most
    -- recent fact_daily_metrics row.
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

    -- v_trading_days_only: dim_date filtered down to just is_trading_day
    -- = TRUE, for queries that only want to walk actual trading dates.
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

    -- v_top_returns: mart_screener narrowed to just identity + return
    -- columns, for "top N by return" style queries without extra fields.
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

    -- v_unusual_volume: every fact_daily_metrics row labeled with a
    -- readable volume_flag, instead of just the outliers mart_unusual_volume keeps.
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

    -- v_sector_comparison: each symbol's own 1-year return next to
    -- its category's average, plus the difference - "beating its sector?"
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

    -- v_normalized_price_comparison: thin passthrough over
    -- mart_normalized_prices - exposes the indexed-to-100 price series.
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

    -- v_drawdown_by_year: fact_drawdown_yearly enriched with the
    -- security's name/category/is_etf, so it's usable standalone.
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

    -- v_security_screener: unfiltered passthrough over mart_screener -
    -- the public-facing name for consumers who shouldn't query the
    -- physical mart table directly.
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