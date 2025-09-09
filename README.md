Trading Results Analysis Challenge
 Project Overview

This project implements data modeling and analysis of trading results for a global multi-asset brokerage.
The main objective is to build clean data marts for analysis and provide management with key metrics on performance, trader activity, and risk/account health.

 Data Sources

Raw data comes in CSV format (loaded as dbt seeds):

trades_raw — trade records (IDs, accounts, clients, symbols, commissions, PnL).

accounts_raw — registry of trading accounts.

clients_raw — client master table.

balances_eod_raw — end-of-day account balances (equity, margin, floating PnL).

symbols_ref — symbol normalization reference.

 Data Modeling

Models are organized into three layers:

Staging (stg_)
Data cleaning, type casting, normalization, handling nulls.

Dimensions (dim_)

dim_client — clients directory.

dim_account — accounts directory (with open/close dates).

dim_symbol — normalized symbols from symbols_ref.

dim_date — calendar table.

Facts (fact_)

fact_trades — all trades, with recalculated PnL and commission, trade result (PROFIT/LOSS), is_post_close_trade flag.

fact_account_eod — daily account snapshots: balance, equity, margin, drawdown, free margin, margin status.

f_client_performance_daily — client-level daily summary: PnL, commissions, activity, balances, risk indicators.

 Business Metrics

Implemented:

Performance

Top 10 best and worst clients/symbols by net_pnl.

Symbols contributing most to losses.

Activity

Number of active traders (daily/weekly).

Average trade size by symbol and client segment.

Risk / Account Health

Clients/accounts with the largest equity drawdown (max_drawdown_abs, max_drawdown_pct).

Accounts flagged is_deleted = true but still trading (is_post_close_trade = true).

Margin risk classification (SAFE / WARNING / RISK / CRITICAL).

 Data Quality Tests

Implemented in dbt:

not_null / unique — on key fields (trade_id, account_id, client_id).

accepted_values — for side (BUY / SELL).

relationships — enforcing links between fact_trades and dim_account / dim_client.

custom singular test: test_post_close_trades — counts trades executed after account closure (severity = warn).

 How to Run

Install dependencies:

dbt deps

Run models:

dbt run

or selectively:

dbt run --select fact_trades


Run tests:

dbt test
or 
dbt test --select test_post_close_trades

 Outputs

Data marts (schemas fact and dim).

Daily client performance summary for BI dashboards 

Data quality tests to highlight inconsistencies and edge cases.

 