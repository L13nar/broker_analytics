{{ config(materialized='table') }}

select distinct
    s.platform,
    s.platform_symbol,
    s.std_symbol,
    s.asset_class,
    s.quote_currency,
    s.tick_value
from {{ ref('stg_symbols') }} s
