{{ config(materialized='table') }}

with trades_daily as (
    select
        client_id,
        date_trunc('day', coalesce(close_time, open_time)) as trade_date,
        sum(coalesce(realized_pnl,0))        as total_pnl,
        sum(coalesce(commission_amt,0))          as total_commission,
        sum(coalesce(net_pnl,0))             as net_pnl,
        count(distinct trade_id)             as trades_count,
        avg(coalesce(net_pnl,0))             as avg_pnl_per_trade
    from {{ ref('fact_trades') }}
    group by client_id, date_trunc('day', coalesce(close_time, open_time))
),

balances_window as (
    select
        client_id,
        eod_date,
        balance,
        equity,
        floating_pnl,
        credit,
        margin_level,
        floating_pnl_pct,
        pnl_status,
        free_margin,
        margin_status,
        row_number() over (
            partition by client_id, eod_date
            order by eod_date desc
        ) as rn
    from {{ ref('fact_account_eod') }}
),

balances_daily as (
    select
        client_id,
        eod_date,
        balance,
        equity,
        floating_pnl,
        credit,
        margin_level,
        floating_pnl_pct,
        pnl_status,
        free_margin,
        margin_status
    from balances_window
    where rn = 1
)

select
    coalesce(t.client_id, b.client_id)       as client_id,
    coalesce(t.trade_date, b.eod_date)       as report_date,

    t.total_pnl,
    t.total_commission,
    t.net_pnl,
    t.trades_count,
    t.avg_pnl_per_trade,

    b.balance,
    b.equity,
    b.floating_pnl,
    b.credit,
    b.margin_level,

    b.floating_pnl_pct,
    b.pnl_status,
    b.free_margin,
    b.margin_status

from trades_daily t
full outer join balances_daily b
    on t.client_id = b.client_id
   and t.trade_date = b.eod_date
