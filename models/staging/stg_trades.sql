{{ config(materialized='table') }}

with src as (
  select
    trim(trade_id)                        as trade_id,
    lower(trim(platform))                 as platform,
    trim(account_id)                      as account_id,
    nullif(trim(client_external_id),'')   as client_external_id,   
    trim(symbol)                          as platform_symbol,
    case
      when upper(trim(side)) in ('BUY','B')  then 'BUY'
      when upper(trim(side)) in ('SELL','S') then 'SELL'
      else null
    end                                   as side,
    try_cast(volume       as double)      as volume,
    try_cast(open_time    as timestamp)   as open_time,
    try_cast(close_time   as timestamp)   as close_time,
    try_cast(open_price   as double)      as open_price,
    try_cast(close_price  as double)      as close_price,
    try_cast(commission   as double)      as commission,
    try_cast(realized_pnl as double)      as realized_pnl,
    coalesce(nullif(trim(book_flag), ''), 'UNKNOWN') as book_flag,
    coalesce(nullif(trim(counterparty), ''), 'UNKNOWN') as counterparty,
    upper(trim(quote_currency))           as quote_currency,
    upper(trim(status))                   as status
  from {{ ref('trades_raw') }}
)

select
  *
from src