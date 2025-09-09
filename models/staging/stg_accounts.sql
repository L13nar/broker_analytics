{{ config(materialized='table') }}

select
  trim(account_id)                as account_id,
  lower(trim(platform))           as platform,
  trim(client_id)                 as client_id,
  upper(trim(base_currency))      as base_currency,
  try_cast(created_at as timestamp) as created_at,
  try_cast(closed_at  as timestamp) as closed_at,
  trim(salesforce_account_id)     as salesforce_account_id,
  try_cast(is_system as boolean)  as is_system,
  try_cast(is_deleted as boolean) as is_deleted
from {{ ref('accounts_raw') }}
