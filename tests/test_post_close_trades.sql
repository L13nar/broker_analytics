{{ config(severity='warn') }}

select *
from {{ ref('fact_trades') }}
where is_post_close_trade = true
