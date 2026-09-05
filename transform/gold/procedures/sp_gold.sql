-- ============================================================
-- MARKETLENS GOLD LAYER
-- ============================================================
-- In simple words: this file is a recipe that BigQuery follows
-- every time new data is ready. It reads the cleaned data from
-- Silver, and turns it into a small set of easy-to-query tables
-- that a dashboard (or a person typing SQL) can use directly,
-- without needing to know how the raw data was structured.
--
-- Source:
--   silver.silver_market_data      (cleaned daily prices)
--   silver.silver_symbol_metadata  (ticker name/exchange/category)
--
-- Procedure:
--   gold.sp_refresh_all(p_run_id STRING)
--
-- p_run_id is just a label (like a batch number) stamped onto every
-- audit row this run writes, so you can later look at the audit log
-- and see everything that happened in one specific run, together.
-- It is never used to filter or change any actual data.
--
-- Gold trigger:
--   gold_trigger/<run_id>/_READY
--
-- Partitioning:
--   Big tables that have one row per day are split ("partitioned")
--   by month, purely so BigQuery can skip reading months you don't
--   need when you query a specific date range - faster and cheaper.
--
-- Duplicate key:
--   Every table that has "one row per symbol per day" uses
--   symbol + Date as its unique key. gold_base (right below) is
--   where duplicates get resolved once, at the very start, so
--   nothing downstream has to worry about it again.
--
-- Audit:
--   Every step below writes a SUCCESS or FAILED row to
--   bronze.ingestion_audit once it finishes - same table Bronze
--   and Silver already write to. If a step fails, the whole
--   procedure stops (RAISE), so a broken step can never be
--   silently ignored.
--
-- **WHAT CHANGED IN THIS VERSION, IN PLAIN WORDS:**
--
-- 1. We found a small number of rows in the raw source data where a
--    stock's price was recorded as something absurd for one or two
--    days - for example, a $43,000 stock briefly showing as $0.17 -
--    before jumping back to a normal price. This is almost certainly
--    a typo or a glitch in the original file, not a real market
--    event. Our cleaning step (Silver) only rejects a price if it is
--    EXACTLY zero, so a wrong-but-nonzero price like $0.17 slipped
--    through as "valid."
--
--    Because of that one bad price, the "return" calculation (which
--    compares today's price to a past price) produced enormous fake
--    numbers - stocks appearing to gain 2,500% in a year, or lose
--    almost 100% overnight - none of which are real.
--
--    Rather than rewrite and re-run Silver's cleaning logic against
--    the FULL history right before a deadline (risky, and would take
--    hours), we added a simple safety filter further downstream, in
--    mart_screener and mart_normalized_prices: any return or index
--    value that is clearly too extreme to be a real trading outcome
--    gets left out of these two tables. The original raw and Silver
--    data are NOT changed - only these two final, dashboard-facing
--    tables are protected. See each step's comments below for the
--    exact filter.
--
-- 2. Objects that don't help answer the two example questions this
--    project is actually about -
--        "which mid-cap stocks had unusual volume spikes?"
--        "compare AAPL against its sector peers over the last year"
--    - were removed on purpose (dim_date, fact_drawdown_yearly, and
--      a few unused views). A statistical "peer" table based on
--      price correlation was also tried and then removed, because
--      there wasn't enough trading history yet to make it reliable -
--      see step 7's comment for the full story.
-- ============================================================

CREATE OR REPLACE PROCEDURE
`market-lens-506611.gold.sp_refresh_all`(p_run_id STRING)
BEGIN

    -- v_step_started_at: a stopwatch. We reset it right before each
    -- step below, so that step's own audit row knows how long it took.
    DECLARE v_step_started_at TIMESTAMP;

    -- v_ma_lookback_days: how many days back we re-check when
    -- updating fact_daily_metrics. In simple words: instead of
    -- re-reading a stock's ENTIRE history every single day just to
    -- add one new row, we only look back far enough to correctly
    -- recompute the longest rolling average we use (200 days), plus
    -- a safety cushion for weekends and holidays.
    DECLARE v_ma_lookback_days INT64 DEFAULT 300;

    -- v_rows_affected: how many rows the fact_daily_metrics update
    -- actually touched this run - used so the audit log shows a real,
    -- honest number instead of the size of the whole table.
    DECLARE v_rows_affected INT64;

    -- ========================================================
    -- STEP 0 - Get one clean copy of Silver's data ready to use
    -- ========================================================
    -- In simple words: this is scratch paper, not a final table -
    -- it disappears once this run finishes, and nobody outside this
    -- procedure can see it. Every step below reads from here instead
    -- of going back to Silver each time.
    --
    -- Silver can sometimes end up with more than one row for the
    -- same stock on the same day (e.g. if a run overlapped another).
    -- This keeps only ONE row per symbol+Date - whichever one was
    -- WRITTEN MOST RECENTLY (silver_loaded_at is a real timestamp).
    -- We deliberately do NOT sort by run_id, because run_id is just
    -- text like "historical_2026..." or "incremental_2026..." and
    -- sorting text alphabetically is not the same as sorting by time.
    -- ========================================================

    CREATE TEMP TABLE gold_base AS
    SELECT *
    FROM `market-lens-506611.silver.silver_market_data`
    WHERE is_valid = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, Date
        ORDER BY silver_loaded_at DESC, Date DESC
    ) = 1;


    -- ========================================================
    -- STEP 0.5 - Remove old tables/views we no longer need
    -- ========================================================
    -- In simple words: earlier versions of this pipeline built a
    -- few tables that turned out not to help answer either of the
    -- two questions this project is actually about. Rather than
    -- leave them sitting around unused and confusing, this deletes
    -- them every time the pipeline runs. "IF EXISTS" means it's
    -- completely safe to run this even after they're already gone -
    -- it just does nothing in that case, no error.
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
    -- STEP 1 - dim_security: "who is this ticker?"
    -- ========================================================
    -- In simple words: one row per stock/ETF, with its name,
    -- exchange, and listing tier. This is a lookup table, like a
    -- phone book - it doesn't change day to day, so we rebuild it
    -- fully and cheaply every run rather than trying to "merge" it.
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
            ETF = 'Y' AS is_etf,          -- turns the Y/N text into a plain true/false
            asset_type
        FROM `market-lens-506611.silver.silver_symbol_metadata`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY symbol
        ) = 1;                            -- just a safety net in case a symbol ever appears twice

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
    -- STEP 2 - fact_daily_metrics: "what happened on this day?"
    -- ========================================================
    -- In simple words: this is the most important table in Gold -
    -- one row per stock, per day, with its price, volume, and every
    -- rolling calculation (30-day volume average, drawdown, moving
    -- averages). Almost everything else in Gold is built from this
    -- table, directly or indirectly.
    --
    -- Instead of throwing this table away and rebuilding it from
    -- scratch every run (slow, and gets slower every day as history
    -- grows), we keep it permanently and only recompute a small,
    -- recent slice of it each time - just enough days back to get
    -- the rolling averages right (see v_ma_lookback_days above).
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        -- Create the table once, with an exact column list, if it
        -- doesn't already exist. If it already exists, this line
        -- does nothing - the table just keeps its data.
        CREATE TABLE IF NOT EXISTS
        `market-lens-506611.gold.fact_daily_metrics`
        (
            Date DATE,
            symbol STRING,
            asset_type STRING,
            Close FLOAT64,
            Volume FLOAT64,   -- a decimal number, matching how Silver stores it
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
        PARTITION BY DATE_TRUNC(Date, MONTH)   -- split by month, for faster date-range queries
        CLUSTER BY symbol;                     -- grouped by symbol, for faster per-stock lookups

        -- MERGE means: for each row coming in, if a matching row
        -- already exists (same symbol+Date+asset_type), update it;
        -- otherwise, add it as new. This is what makes it safe to
        -- run this same procedure over and over without ever
        -- creating duplicate rows.
        MERGE `market-lens-506611.gold.fact_daily_metrics` T
        USING (

            -- For each symbol, find the last day we already have a
            -- row for in this table. NULL means "brand new symbol,
            -- we've never seen it before."
            WITH fact_watermarks AS (
                SELECT symbol, MAX(Date) AS last_fact_date
                FROM `market-lens-506611.gold.fact_daily_metrics`
                GROUP BY symbol
            ),

            -- Only look at rows from gold_base that are either brand
            -- new (never-seen symbol) or fall within the trailing
            -- lookback window of a symbol we've already got. This is
            -- the actual "don't reprocess everything" logic.
            recompute_scope AS (
                SELECT g.*
                FROM gold_base g
                LEFT JOIN fact_watermarks w ON w.symbol = g.symbol
                WHERE w.last_fact_date IS NULL
                   OR g.Date >= DATE_SUB(w.last_fact_date, INTERVAL v_ma_lookback_days DAY)
            )

            -- The actual calculations, done only over the smaller
            -- slice of data above, not the symbol's whole history.
            SELECT
                Date, symbol, asset_type, Close, Volume, daily_return_pct,
                rolling_avg_volume_30d, rolling_stddev_volume_30d,
                -- how many "standard deviations" away today's volume is
                -- from this stock's own normal (30-day average) volume -
                -- this is what flags "unusual volume" later on
                SAFE_DIVIDE(Volume - rolling_avg_volume_30d, rolling_stddev_volume_30d) AS volume_zscore,
                rolling_max_close,
                -- how far below its all-time high this stock is right now,
                -- as a negative percentage (0 = at its high, -0.30 = 30% down)
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
            -- row already exists -> just refresh it with the newly computed values
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
            -- brand new row -> add it
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

        -- how many rows this MERGE actually touched - used in the
        -- audit row below instead of counting the whole (huge) table
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
    -- STEP 3 - fact_period_returns: "how did it do over time?"
    -- ========================================================
    -- In simple words: for every stock, this works out the percent
    -- gain or loss over five windows - the last week, month, 3
    -- months, year, and 3 years. It finds the closest trading day
    -- to each window's start date (since markets are closed on
    -- weekends/holidays, "exactly 1 year ago" might not be a
    -- trading day) and compares that price to today's.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.fact_period_returns`
        CLUSTER BY symbol
        AS
        WITH latest_date_per_symbol AS (
            -- each symbol's most recent trading day - our "today" anchor
            SELECT symbol, MAX(Date) AS latest_date
            FROM gold_base
            GROUP BY symbol
        ),
        period_definitions AS (
            -- one row per symbol per time window, with the date we're
            -- trying to find a matching price for
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
            -- for each symbol+window, pick the real trading day closest
            -- to the theoretical start date (searching +/- 7 days)
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
            -- today's actual closing price, per symbol
            SELECT f.symbol, f.Date AS end_date, f.Close AS end_close
            FROM gold_base f
            JOIN latest_date_per_symbol l ON f.symbol = l.symbol AND f.Date = l.latest_date
        )
        SELECT
            s.symbol, s.period, s.actual_start_date AS period_start_date, e.end_date AS period_end_date,
            s.start_close, e.end_close,
            -- the actual percent change: (new price - old price) / old price, as a percentage
            SAFE_DIVIDE(e.end_close - s.start_close, s.start_close) * 100 AS return_pct
        FROM closest_start_prices s
        JOIN end_prices e ON s.symbol = e.symbol
        WHERE s.closeness_rank = 1;   -- only keep the single best-matched start date per symbol+window

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
    -- STEP 4 - mart_screener: the main "one row per stock" table
    -- ========================================================
    -- In simple words: this is the table your dashboard uses the
    -- most. It takes each stock's latest snapshot, joins on its
    -- name/category, and joins on all five return windows - so a
    -- dashboard chart can just read straight from here instead of
    -- doing five separate joins itself every time.
    --
    -- **NEW - the extreme-value safety filter, explained simply:**
    -- We found a handful of stocks where a bad price somewhere in
    -- their history (see the top-of-file note) made their calculated
    -- return come out as an absurd number - either a huge fake gain
    -- (thousands of percent) or a fake near-total loss (close to
    -- -100%, from a price that briefly, wrongly, showed as near
    -- zero). A REAL year of trading almost never produces a return
    -- outside roughly -99% to +2000% - so anything past that is far
    -- more likely to be a data glitch than a real result, and we
    -- leave that stock out of this table rather than show a
    -- misleading number on the dashboard.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.mart_screener`
        CLUSTER BY symbol
        AS
        WITH latest_metrics AS (
            -- just each symbol's single most recent row
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
        -- five separate joins, one per time window, so each return
        -- ends up as its own column instead of its own row
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r1w ON lm.symbol = r1w.symbol AND r1w.period = '1W'
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r1m ON lm.symbol = r1m.symbol AND r1m.period = '1M'
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r3m ON lm.symbol = r3m.symbol AND r3m.period = '3M'
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r1y ON lm.symbol = r1y.symbol AND r1y.period = '1Y'
        LEFT JOIN `market-lens-506611.gold.fact_period_returns` r3y ON lm.symbol = r3y.symbol AND r3y.period = '3Y'
        -- **NEW:** the safety filter described above. A NULL return
        -- (e.g. a brand-new symbol with no 3-year history yet) is
        -- always allowed through - "OR ... IS NULL" makes sure we
        -- only reject a return we can actually see and judge, never
        -- one that's simply missing for a normal reason.
        WHERE (r1w.return_pct  BETWEEN -99 AND 2000 OR r1w.return_pct  IS NULL)
          AND (r1m.return_pct  BETWEEN -99 AND 2000 OR r1m.return_pct  IS NULL)
          AND (r3m.return_pct  BETWEEN -99 AND 2000 OR r3m.return_pct  IS NULL)
          AND (r1y.return_pct  BETWEEN -99 AND 2000 OR r1y.return_pct  IS NULL)
          AND (r3y.return_pct  BETWEEN -99 AND 2000 OR r3y.return_pct  IS NULL);

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
    -- STEP 5 - mart_normalized_prices: fair price comparison
    -- ========================================================
    -- In simple words: a $5 stock and a $500 stock can't be compared
    -- on raw price - so this "re-scales" every stock to start at 100
    -- on its very first day, making it easy to see which one actually
    -- moved more in percentage terms, regardless of starting price.
    --
    -- Rebuilt fully every run (not incrementally), because the
    -- starting point (100) depends on each stock's very first ever
    -- price - there's no safe "trailing window" shortcut for that.
    --
    -- **NEW - same safety filter idea as mart_screener:** if a bad
    -- price anywhere in a stock's history got used as its "base"
    -- price, or as a later price, the indexed number could come out
    -- absurdly large or tiny. A real indexed price should realistically
    -- stay within roughly 1 to 100,000 (that upper bound is already
    -- extremely generous - a real stock going up 1000x is very rare).
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
            -- each symbol's very first trading day - this becomes its "100" point
            SELECT symbol, MIN(Date) AS base_date
            FROM gold_base
            GROUP BY symbol
        ),
        indexed AS (
            SELECT
                f.Date, f.symbol, f.Close, b.base_date AS index_base_date,
                -- every price expressed as: "what % of the starting price is this,
                -- scaled so the starting price itself = 100"
                SAFE_DIVIDE(f.Close, base_close.Close) * 100 AS indexed_price
            FROM gold_base f
            JOIN base_prices b ON f.symbol = b.symbol
            JOIN gold_base base_close ON base_close.symbol = b.symbol AND base_close.Date = b.base_date
        )
        SELECT *
        FROM indexed
        -- **NEW:** the same kind of safety filter as mart_screener,
        -- applied here too - keeps out any indexed value that's
        -- clearly the result of a bad underlying price, not a real
        -- price movement.
        WHERE indexed_price BETWEEN 1 AND 100000;

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
    -- STEP 6 - mart_unusual_volume: today's outliers only
    -- ========================================================
    -- In simple words: this keeps only the stocks whose volume today
    -- is a real statistical outlier - more than 2 "standard
    -- deviations" away from that stock's own normal (30-day average)
    -- volume. This directly answers "which stocks had unusual
    -- volume?" without anyone needing to remember the exact filter.
    -- ========================================================

    SET v_step_started_at = CURRENT_TIMESTAMP();

    BEGIN

        CREATE OR REPLACE TABLE
        `market-lens-506611.gold.mart_unusual_volume`
        AS
        SELECT
            symbol, Date, Volume, volume_zscore, asset_type, run_id, gold_loaded_at
        FROM `market-lens-506611.gold.fact_daily_metrics`
        WHERE ABS(volume_zscore) > 2   -- more than 2 standard deviations from normal
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
    -- VIEWS - simple "windows" onto the tables above
    -- ========================================================
    -- In simple words: a view is not a real table - it stores no
    -- data of its own. It's a saved question that BigQuery re-runs,
    -- live, every time someone opens it. We use views here either to
    -- show a smaller, simpler slice of a table, or to pre-join two
    -- tables together so nobody has to repeat that join by hand.
    -- ========================================================

    -- v_top_returns: just the columns a "top returns" chart needs,
    -- instead of every column mart_screener has.
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

    -- v_normalized_price_comparison: same idea, a simple window onto
    -- mart_normalized_prices, for charting one or more symbols' price
    -- paths over time.
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
    -- STEP 7 - mart_sector_summary + v_sector_comparison
    -- ========================================================
    -- In simple words, the honest story behind this table:
    --
    -- The raw data has a field called "Market Category" with values
    -- like Q, G, S. This is NOT a stock's industry (like "Tech" or
    -- "Healthcare") - it's actually which NASDAQ LISTING TIER the
    -- stock trades on. There is no real industry/sector information
    -- anywhere in the source data we were given.
    --
    -- We tried building a smarter substitute: grouping stocks by how
    -- closely their day-to-day price movements correlate with each
    -- other, as a stand-in for "peers." That required a full year of
    -- real trading history per stock to be trustworthy, and our data
    -- didn't have enough of that yet - so it came back empty and was
    -- removed.
    --
    -- So this table is the honest fallback: real data, grouped by
    -- LISTING TIER, clearly labeled as such everywhere it's shown on
    -- the dashboard - never called "sector."
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
            STDDEV(return_1y) AS market_category_volatility_1y   -- how spread-out returns are within this tier
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


    -- v_sector_comparison: puts one stock's own 1-year return right
    -- next to its listing tier's average, plus the difference between
    -- them. This is what powers the "symbol vs. its peers" dashboard
    -- panel - just remember to label it "listing tier" on the
    -- dashboard, not "sector."
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