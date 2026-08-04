-- Global Data Mart gold layer: materialized views, gross margin, and dynamic tables
-- Co-authored with CoCo
-- Global Data Mart — GOLD LAYER (missing pieces)
-- Run AFTER the ingestion script (Bronze/Silver setup) and the dimensional model script
-- (dim_date, dim_store, dim_product, fct_daily_sales) have already been run once.

use database global_data_mart;
use warehouse compute_wh;

-- ============================================================
-- 1. Transient buffer + incremental refresh task chain
--    (Silver -> Gold, event-driven instead of full rebuild)
-- ============================================================

create or replace transient table global_data_mart.setup_schema.daily_sales_buffer as
select transaction_date as date_key, store_id, product_sku, category,
    count(*) as total_transactions, sum(quantity) as total_quantity,
    sum(total_amount) as total_revenue
from global_data_mart.setup_schema.pos_transactions
group by transaction_date, store_id, product_sku, category;

create or replace task global_data_mart.setup_schema.task_pos_incremental
warehouse = compute_wh
schedule = '5 minute'
when system$stream_has_data('global_data_mart.setup_schema.stream_pos_new')
as
insert into global_data_mart.setup_schema.pos_transactions_processed (
    transaction_id, store_id, store_name, store_city, store_region,
    cashier_id, customer_id, transaction_date, transaction_time,
    product_sku, product_name, category, subcategory, quantity,
    unit_price, discount_pct, total_amount, payment_method, loyalty_points
)
select
    transaction_id, store_id, store_name, store_city, store_region,
    cashier_id, customer_id, transaction_date, transaction_time,
    product_sku, product_name, category, subcategory, quantity,
    unit_price, discount_pct, total_amount, payment_method, loyalty_points
from global_data_mart.setup_schema.stream_pos_new;

create or replace task global_data_mart.setup_schema.task_refresh_sales_buffer
warehouse = compute_wh
after global_data_mart.setup_schema.task_pos_incremental
as
create or replace transient table global_data_mart.setup_schema.daily_sales_buffer as
select transaction_date as date_key, store_id, product_sku, category,
    count(*) as total_transactions, sum(quantity) as total_quantity,
    sum(total_amount) as total_revenue
from global_data_mart.setup_schema.pos_transactions
group by transaction_date, store_id, product_sku, category;

-- Activation order matters: resume CHILD first, then ROOT last,
-- otherwise the child never runs.
alter task global_data_mart.setup_schema.task_refresh_sales_buffer resume;
alter task global_data_mart.setup_schema.task_pos_incremental resume;

-- Verify tasks are running:
-- select name, state, scheduled_time, error_message
-- from table(information_schema.task_history())
-- order by scheduled_time desc;


-- ============================================================
-- 2. Materialized Views
-- ============================================================

create or replace materialized view global_data_mart.setup_schema.mv_store_daily_revenue as
select store_id, store_name, transaction_date,sum(total_amount) as daily_revenue from global_data_mart.setup_schema.pos_transactions
group by store_id, store_name, transaction_date;

create or replace view global_data_mart.setup_schema.v_store_revenue_30d as
select store_id, store_name,sum(daily_revenue) as revenue_30d from global_data_mart.setup_schema.mv_store_daily_revenue
where transaction_date >= dateadd(day, -30, current_date())
group by store_id, store_name;

create or replace materialized view global_data_mart.setup_schema.mv_category_by_region as
select store_region, category,sum(total_amount) as total_revenue,sum(quantity) as total_units from global_data_mart.setup_schema.pos_transactions
group by store_region, category;


select * from global_data_mart.setup_schema.mv_category_by_region order by total_revenue desc;


-- ============================================================
-- 3. Gross Margin view (POS revenue vs ERP cost)
--    Note: joins on store_id + category, NOT product_sku —
--    POS and ERP use different SKU schemes by design.
-- ============================================================

create or replace view global_data_mart.setup_schema.v_gross_margin as
select p.store_id, p.category,
    sum(p.total_amount) as total_revenue,
    sum(e.total_cost) as total_cost,
    sum(p.total_amount) - sum(e.total_cost) as gross_margin,
    round((sum(p.total_amount) - sum(e.total_cost)) / nullif(sum(p.total_amount), 0) * 100, 2) as gross_margin_pct
from global_data_mart.setup_schema.pos_transactions p
join global_data_mart.setup_schema.erp_orders e
    on p.store_id = e.store_id
   and p.category = e.category
group by p.store_id, p.category;

select * from global_data_mart.setup_schema.v_gross_margin order by gross_margin_pct;


-- ============================================================
-- 4. IoT Alerts -> Sales Impact
--    LATERAL FLATTEN on the alerts[] array (present on ~5% of events)
-- ============================================================

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

create or replace view global_data_mart.setup_schema.v_alert_sales_impact as
select a.store_id, date(a.triggered_at) as alert_date, a.alert_type, a.severity,
    count(*) as alert_count,
    sum(p.total_amount) as same_day_revenue
from global_data_mart.setup_schema.v_iot_alerts a
left join global_data_mart.setup_schema.pos_transactions p
    on a.store_id = p.store_id
   and date(a.triggered_at) = p.transaction_date
group by a.store_id, date(a.triggered_at), a.alert_type, a.severity
order by alert_date;

select * from global_data_mart.setup_schema.v_iot_alerts limit 20;
select * from global_data_mart.setup_schema.v_alert_sales_impact order by alert_date;
