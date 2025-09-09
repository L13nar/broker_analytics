{{ config(materialized='table') }}

with balances as (
    select
        b.account_id,
        a.client_id,
        b.platform,
        b.date as eod_date,
        b.balance,
        b.equity,
        b.floating_pnl,
        b.credit,
        b.margin_level,

        round(100 * nullif(b.floating_pnl,0) / nullif(b.equity,0), 2) as floating_pnl_pct,
        case
        when round(100 * b.floating_pnl / nullif(b.equity - b.floating_pnl,0), 1) <= -50 then 'CRITICAL'
        when round(100 * b.floating_pnl / nullif(b.equity - b.floating_pnl,0), 1) <= -20 then 'RISK'
        when round(100 * b.floating_pnl / nullif(b.equity - b.floating_pnl,0), 1) < 0 then 'WARNING'
        when round(100 * b.floating_pnl / nullif(b.equity - b.floating_pnl,0), 1) >= 20 then 'GOOD'
        else 'NEUTRAL'
    end as pnl_status,
        (b.equity - b.credit) as free_margin,
           case
          when b.equity <= 0 then 'CRITICAL'                
          when b.equity <= b.credit then 'RISK'             
          when b.equity >= b.credit * 2 then 'SAFE'         
          else 'WARNING'                                    
          end as margin_status
    from {{ ref('stg_balances_eod') }} b
    left join {{ ref('dim_account') }} a
        on b.account_id = a.account_id
)

select *
from balances
