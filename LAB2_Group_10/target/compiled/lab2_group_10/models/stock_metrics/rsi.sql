WITH base AS (
    SELECT 
        symbol,
        date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
    FROM USER_DB_MARMOT.analytics.raw_stocks_data
),

gains_losses AS (
    SELECT
        symbol,
        date,
        CASE WHEN close - prev_close > 0 THEN close - prev_close ELSE 0 END AS gain,
        CASE WHEN close - prev_close < 0 THEN ABS(close - prev_close) ELSE 0 END AS loss
    FROM base
    WHERE prev_close IS NOT NULL
),

rsi_calc AS (
    SELECT
        symbol,
        date,
        -- 14-day moving averages
        AVG(gain) OVER (
            PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain_14d,
        AVG(loss) OVER (
            PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss_14d,

        -- 50-day moving averages
        AVG(gain) OVER (
            PARTITION BY symbol ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS avg_gain_50d,
        AVG(loss) OVER (
            PARTITION BY symbol ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS avg_loss_50d
    FROM gains_losses
)

SELECT
    symbol,
    date,
    ROUND(100 - (100 / (1 + (avg_gain_14d / NULLIF(avg_loss_14d, 0)))), 2) AS rsi_14d,
    ROUND(100 - (100 / (1 + (avg_gain_50d / NULLIF(avg_loss_50d, 0)))), 2) AS rsi_50d
FROM rsi_calc