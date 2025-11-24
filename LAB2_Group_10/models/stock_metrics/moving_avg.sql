-- models/stock_metrics/moving_avg.sql
WITH base AS (
    SELECT 
        symbol,
        date,
        close,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS row_num
    FROM {{ ref('raw_stocks_data') }}
),

moving_avg_calc AS (
    SELECT 
        symbol,
        date,
        round(close, 2) AS close,
        ROUND(AVG(close) OVER (PARTITION BY symbol ORDER BY row_num ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 2) AS moving_avg_14d,
        ROUND(AVG(close) OVER (PARTITION BY symbol ORDER BY row_num ROWS BETWEEN 49 PRECEDING AND CURRENT ROW), 2) AS moving_avg_50d
    FROM base
)

SELECT * FROM moving_avg_calc
