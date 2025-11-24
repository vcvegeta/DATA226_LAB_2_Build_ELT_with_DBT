-- snapshots/stock_snapshot.sql

{% snapshot stock_snapshot %}

{{
  config(
    target_schema = 'snapshots',          
    unique_key   = "symbol || '-' || date::string",
    strategy     = 'check',
    check_cols   = ['open', 'high', 'low', 'close', 'volume']
  )
}}

select
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume
from {{ ref('raw_stocks_data') }}

{% endsnapshot %}
