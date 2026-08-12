use database global_data_mart;


-- ================================================================
-- part 1 — supporting objects your actual tables are missing
-- ================================================================

-- dim_supplier — built from erp_orders since you have no supplier dimension yet
create or replace table global_data_mart.mart.dim_supplier as
select distinct supplier_id, supplier_name, supplier_city
from global_data_mart.setup_schema.erp_orders;

-- extended dim_date — your original only has date_key, day, month
create or replace table global_data_mart.mart.dim_date as
select distinct
    transaction_date                         as date_key,
    day(transaction_date)                    as day,
    month(transaction_date)                  as month_number,
    monthname(transaction_date)              as month_name,
    year(transaction_date)                   as year,
    quarter(transaction_date)                as quarter_number,
    'Q' || quarter(transaction_date)          as quarter_name,
    to_char(transaction_date, 'YYYY-MM')      as month_year
from global_data_mart.setup_schema.pos_transactions;


-- ================================================================
-- part 2 — materialized views
-- ================================================================

create or replace materialized view global_data_mart.mart.mv_store_revenue as
select
    store_id, store_name, store_city, store_region, report_date,
    round(sum(total_revenue), 2) as revenue,
    sum(total_transaction)       as txns,
    round(avg(avg_basket), 2)    as avg_basket,
    sum(unique_customer)         as customers
from global_data_mart.mart.fact_daily_sales
group by store_id, store_name, store_city, store_region, report_date;

select dateadd('day', -30, max(report_date)) from global_data_mart.mart.fact_daily_sales;

select store_name, store_city,
    sum(revenue)   as revenue_last_30d,
    sum(txns)      as txns_last_30d,
    sum(customers) as customers_last_30d
from global_data_mart.mart.mv_store_revenue
where report_date >= (
    select dateadd('day', -30, max(report_date))
    from global_data_mart.mart.fact_daily_sales
)
group by store_name, store_city
order by revenue_last_30d desc;

create or replace materialized view global_data_mart.mart.mv_category_revenue as
select
    category, store_region,
    round(sum(total_revenue), 2) as revenue,
    sum(total_units)             as units_sold,
    sum(total_transaction)       as transactions,
    count(*)                     as row_count
from global_data_mart.mart.fact_daily_sales
group by category, store_region;

select * from global_data_mart.mart.mv_category_revenue order by revenue desc;

create or replace materialized view global_data_mart.mart.mv_margin_summary as
select
    store_id, store_name, store_city,
    round(sum(total_revenue), 2)    as total_revenue,
    round(sum(total_cost), 2)       as total_cost,
    round(sum(gross_profit), 2)     as total_profit,
    round(avg(gross_margin_pct), 1) as avg_margin_pct,
    sum(total_units_sold)           as total_units
from global_data_mart.mart.fct_gross_margin
group by store_id, store_name, store_city;

select store_name, store_city, total_revenue, total_cost, total_profit, avg_margin_pct
from global_data_mart.mart.mv_margin_summary
order by avg_margin_pct desc;


-- ================================================================
-- part 3 — regular views (mv restrictions: count distinct, flatten)
-- ================================================================

create or replace view global_data_mart.mart.v_category_revenue as
select
    category, store_region,
    round(sum(total_revenue), 2) as revenue,
    sum(total_units)             as units_sold,
    count(distinct store_id)     as stores_selling
from global_data_mart.mart.fact_daily_sales
group by category, store_region;

select * from global_data_mart.mart.v_category_revenue order by revenue desc;

create or replace view global_data_mart.setup_schema.v_iot_alerts as
select
    r.data:event_id::string as event_id,
    r.data:store_id::string as store_id,
    r.data:timestamp::timestamp as event_timestamp,
    a.value:alert_type::string as alert_type,
    a.value:severity::string as severity,
    a.value:triggered_at::timestamp as triggered_at
from global_data_mart.setup_schema.iot_events_raw r,
    lateral flatten(input => r.data:alerts) a;

select store_id, alert_type, severity, count(*) as alert_count
from global_data_mart.setup_schema.v_iot_alerts
group by store_id, alert_type, severity
order by alert_count desc;


-- ================================================================
-- part 4 — analytics queries
-- ================================================================

-- query 1: revenue by category with ranking
select
    category,
    round(sum(total_revenue), 2) as total_revenue,
    sum(total_units)             as total_units_sold,
    sum(total_transaction)       as total_transactions,
    round(avg(avg_basket), 2)    as avg_basket_size,
    rank() over (order by sum(total_revenue) desc) as revenue_rank
from global_data_mart.mart.fact_daily_sales
group by category
order by revenue_rank;

-- query 2: daily revenue trend with 7-day moving average
use database global_data_mart;
use warehouse compute_wh;

select
    report_date, store_name,
    sum(total_revenue) as daily_revenue,
    round(
        avg(sum(total_revenue)) over (
            partition by store_name
            order by report_date
            rows between 6 preceding and current row
        ), 2
    ) as revenue_7d_ma,
    round(
        100.0 * (
            sum(total_revenue)
            - lag(sum(total_revenue)) over (partition by store_name order by report_date)
        )
        / nullif(lag(sum(total_revenue)) over (partition by store_name order by report_date), 0)
    , 1) as day_over_day_pct
from global_data_mart.mart.fact_daily_sales
group by report_date, store_name
order by store_name, report_date;

-- query 3: store ranking with percentile and % of total revenue
select
    store_name, store_city, category,
    total_revenue, total_cost, gross_profit, gross_margin_pct,
    rank() over (partition by category order by gross_margin_pct desc) as rank_in_category,
    ntile(4) over (order by total_revenue desc) as revenue_quartile,
    round(100.0 * total_revenue / sum(total_revenue) over (), 2) as pct_of_total_revenue,
    case
        when gross_margin_pct >= 35 then 'High'
        when gross_margin_pct >= 30 then 'Medium'
        when gross_margin_pct >= 25 then 'Low'
        else 'Loss Risk'
    end as margin_band
from global_data_mart.mart.fct_gross_margin
order by category, rank_in_category;

-- query 4: iot vs revenue (using real iot columns only — battery/signal, no temp/power/queue)
select
    p.store_name,
    p.report_date,
    p.category,
    p.total_revenue,
    p.total_transaction as total_txns,
    i.avg_battery_pct,
    i.min_battery_pct,
    i.avg_signal_rssi,
    round(
        p.total_revenue - avg(p.total_revenue) over (partition by p.store_id)
    , 2) as revenue_vs_store_avg,
    round(
        100.0 * (p.total_revenue - avg(p.total_revenue) over (partition by p.store_id))
        / nullif(avg(p.total_revenue) over (partition by p.store_id), 0)
    , 1) as revenue_delta_pct
from global_data_mart.mart.fact_daily_sales p
left join global_data_mart.mart.fct_store_iot_daily i
    on p.store_id = i.store_id
   and p.report_date = i.event_date
order by revenue_vs_store_avg asc;

-- query 5: monthly revenue mom comparison
with monthly as (
    select
        d.month_name, d.month_number, d.quarter_name, d.year, d.month_year,
        sum(f.total_revenue) as revenue,
        sum(f.total_transaction) as transactions,
        sum(f.total_units) as units
    from global_data_mart.mart.fact_daily_sales f
    join global_data_mart.mart.dim_date d
        on f.report_date = d.date_key
    group by d.month_name, d.month_number, d.quarter_name, d.year, d.month_year
)
select
    month_year, month_name, quarter_name,
    round(revenue, 2) as monthly_revenue,
    round(transactions, 0) as monthly_txns,
    round(lag(revenue) over (order by year, month_number), 2) as prev_month_revenue,
    round(
        100.0 * (revenue - lag(revenue) over (order by year, month_number))
        / nullif(lag(revenue) over (order by year, month_number), 0)
    , 1) as mom_growth_pct
from monthly
order by year, month_number;

-- query 6: supplier performance from erp
select
    e.supplier_id,
    s.supplier_name,
    s.supplier_city,
    e.category,
    count(distinct e.order_id) as total_orders,
    sum(e.quantity_ordered) as total_qty_ordered,
    sum(e.quantity_received) as total_qty_received,
    round(sum(e.total_cost), 2) as total_procurement_cost,
    round(avg(e.lead_time_days), 1) as avg_lead_time_days,
    sum(case when e.is_late = false then 1 else 0 end) as on_time_deliveries,
    round(
        100.0 * sum(case when e.is_late = false then 1 else 0 end)
        / nullif(count(distinct e.order_id), 0)
    , 1) as on_time_rate_pct,
    count(case when e.order_status = 'Delayed' then 1 end) as delayed_orders
from global_data_mart.setup_schema.erp_orders e
left join global_data_mart.mart.dim_supplier s
    on e.supplier_id = s.supplier_id
where e.order_id is not null
group by e.supplier_id, s.supplier_name, s.supplier_city, e.category
order by on_time_rate_pct desc;

-- query 7: device battery health — from your actual iot_events table
select distinct
    store_name, store_id, device_id, event_type,
    battery_pct, firmware, store_floor,
    case
        when battery_pct < 10 then 'CRITICAL'
        when battery_pct < 20 then 'LOW'
        else 'OK'
    end as battery_status,
    datediff('day',
        max(date(event_timestamp)) over (partition by device_id),
        (select max(date(event_timestamp)) from global_data_mart.setup_schema.iot_events)
    ) as days_since_last_seen
from global_data_mart.setup_schema.iot_events
where battery_pct < 20
order by battery_pct asc, store_name;

-- query 8: master row count — all layers
select 'dim  : dim_store'        as layer_table, count(*) as rows1 from global_data_mart.setup_schema.dim_store
union all select 'dim  : dim_product',      count(*) from global_data_mart.setup_schema.dim_product
union all select 'dim  : dim_date',         count(*) from global_data_mart.mart.dim_date
union all select 'dim  : dim_supplier',     count(*) from global_data_mart.mart.dim_supplier
union all select 'fact : fact_daily_sales', count(*) from global_data_mart.mart.fact_daily_sales
union all select 'fact : fct_gross_margin', count(*) from global_data_mart.mart.fct_gross_margin
union all select 'fact : fct_store_iot_daily', count(*) from global_data_mart.mart.fct_store_iot_daily
union all select 'mv   : mv_store_revenue', count(*) from global_data_mart.mart.mv_store_revenue
union all select 'mv   : mv_category_revenue', count(*) from global_data_mart.mart.mv_category_revenue
union all select 'mv   : mv_margin_summary', count(*) from global_data_mart.mart.mv_margin_summary
order by 1;


-- ================================================================
-- part 5 — task monitoring
-- ================================================================

select
    name, database_name, schema_name, state,
    scheduled_time, completed_time,
    return_value, error_code, error_message
from table(information_schema.task_history(
    scheduled_time_range_start => dateadd('hour', -1, current_timestamp())
))
where database_name = 'GLOBAL_DATA_MART'
  and state in ('SUCCEEDED', 'FAILED')
order by scheduled_time desc;

show tasks in account;

alter task global_data_mart.setup_schema.erp_orders_task suspend;
alter task global_data_mart.setup_schema.iot_task suspend;