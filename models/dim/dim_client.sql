{{ config(materialized='table') }}

select distinct
    c.client_id,
    c.client_external_id,
    c.jurisdiction,
    c.segment,
    cast(c.created_at as date) as registration_date
from {{ ref('stg_clients') }} c
