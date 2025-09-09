{{ config(materialized='table') }}

select
  lower(trim(platform))     as platform,
  trim(platform_symbol)     as platform_symbol,
  upper(trim(std_symbol))   as std_symbol,
  upper(trim(asset_class))  as asset_class,
  upper(trim(quote_currency)) as quote_currency,
  try_cast(tick_value as double) as tick_value
from {{ ref('symbols_ref') }}