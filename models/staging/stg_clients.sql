{{ config(materialized='table') }}

with src as (
  select
    trim(client_id)          as client_id,
    trim(client_external_id) as client_external_id,
    upper(trim(jurisdiction)) as jurisdiction_raw,
    upper(trim(segment))      as segment_upper,   
    try_cast(created_at as timestamp) as created_at
  from {{ ref('clients_raw') }}
)

select
  client_id,
  client_external_id,
coalesce(jurisdiction_raw, 'UNKNOWN') as jurisdiction,
  segment_upper as segment,
  created_at
from src