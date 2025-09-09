{{ config(materialized='table') }}

select
  cast(date as date) as date,
  trim(account_id)                as account_id,
  lower(trim(platform))           as platform,
  try_cast(balance as double)     as balance,
  try_cast(equity as double)      as equity,
  try_cast(floating_pnl as double) as floating_pnl,
  try_cast(credit as double)      as credit,
  try_cast(margin_level as double) as margin_level
from {{ ref('balances_eod_raw') }}