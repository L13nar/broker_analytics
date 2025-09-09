{{ config(materialized='table') }}

select distinct
    a.account_id,
    a.platform,
    a.client_id,
    a.base_currency,
    a.is_system,
    a.is_deleted,
    cast(a.created_at as date) as account_open_date,
    cast(a.closed_at as date) as account_close_date,
    case 
  when closed_at is null then 'ACTIVE'
  else 'CLOSED'
end as account_status
from {{ ref('stg_accounts') }} a
