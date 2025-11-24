WITH returns AS (
    SELECT 
        symbol,
        date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
    FROM {{ ref('raw_stocks_data') }}
),

daily_returns AS (
    SELECT 
        symbol,
        date,
        (close - prev_close) / prev_close AS daily_return
    FROM returns
    WHERE prev_close IS NOT NULL
)

SELECT
    symbol,
    date,
    round(STDDEV_SAMP(daily_return) OVER (
        PARTITION BY symbol ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ), 2) AS volatility_10d
FROM daily_returns
