{{ config(materialized='table') }}

with trades as (
    select
        t.trade_id,
        t.platform,
        t.book_flag,
        t.counterparty,
        t.quote_currency,
        t.status,
        c.client_id,
        t.account_id,
        s.std_symbol as symbol,
        t.side,
        t.volume,
        s.tick_value,
        t.open_time,
        t.close_time,
        t.open_price,
        t.close_price,
        t.realized_pnl,
        case
         when upper(t.side) = 'BUY'  
         then (t.close_price - t.open_price) * t.volume * s.tick_value
         when upper(t.side) = 'SELL' 
         then (t.open_price - t.close_price) * t.volume * s.tick_value
         else null
         end as realized_pnl_corrected,
        t.commission as commission_pct,
        abs(t.volume * s.tick_value * t.commission) as commission_amt,
         case
         when upper(t.side) = 'BUY'  
         then (t.close_price - t.open_price) * t.volume * s.tick_value
              - (abs(t.volume) * s.tick_value * abs(t.commission))
         when upper(t.side) = 'SELL' 
         then (t.open_price - t.close_price) * t.volume * s.tick_value
              - (abs(t.volume) * s.tick_value * abs(t.commission))
          else 0
         end as net_pnl,
        datediff('day', t.open_time, t.close_time) as trade_duration_days,
        case
    when upper(t.side) = 'BUY'  and t.close_price > t.open_price then 'PROFIT'
    when upper(t.side) = 'BUY'  and t.close_price <= t.open_price then 'LOSS'
    when upper(t.side) = 'SELL' and t.close_price < t.open_price then 'PROFIT'
    when upper(t.side) = 'SELL' and t.close_price >= t.open_price then 'LOSS'
    else 'UNKNOWN'
end as trade_result,
         case
            when a.account_close_date is not null
             and t.close_time > a.account_close_date
            then true
            else false
         end as is_post_close_trade
 from {{ ref('stg_trades') }} t
left join {{ ref('dim_account') }} a
    on t.account_id = a.account_id
left join {{ ref('dim_client') }} c
    on  a.client_id = c.client_id
left join {{ ref('dim_symbol') }} s
    on t.platform_symbol = s.platform_symbol and t.platform=s.platform
) 
select *
from trades
