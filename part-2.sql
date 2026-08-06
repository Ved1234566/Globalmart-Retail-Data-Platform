
use database global_data_mart;
use warehouse compute_wh;

-- ============ processed tables (SILVER LAYER) ============

create or replace table global_data_mart.setup_Schema.iot_events_processed (
    data variant,
    processed_ts timestamp_ntz default current_timestamp()
);

create or replace table global_data_mart.setup_Schema.pos_transactions_processed (
    transaction_id string,
    store_id string,
    store_name string,
    store_city string,
    store_region string,
    cashier_id string,
    customer_id string,
    transaction_date date,
    transaction_time string,
    product_sku string,
    product_name string,
    category string,
    subcategory string,
    quantity int,
    unit_price float,
    discount_pct int,
    total_amount float,
    payment_method string,
    loyalty_points int
);

create or replace table global_data_mart.setup_Schema.erp_orders_processed (
    order_id string,
    order_date timestamp_ntz,
    store_id string,
    store_city string,
    supplier_id string,
    supplier_name string,
    supplier_city string,
    product_sku string,
    category string,
    quantity_ordered int,
    quantity_received int,
    unit_cost float,
    total_cost float,
    order_status string,
    expected_delivery date,
    actual_delivery date,
    warehouse_id string,
    lead_time_days int,
    is_late boolean
);

create or replace table global_data_mart.setup_Schema.erp_inventory_processed (
    snapshot_date date,
    store_id string,
    warehouse_id string,
    product_sku string,
    category string,
    quantity_on_hand int,
    reorder_level int,
    max_stock_level int,
    last_received_date date
);

-- ============ tasks (BRONZE -> SILVER) ============

create or replace task global_data_mart.setup_Schema.pos_task
warehouse = compute_wh
schedule = '5 minute'
as
insert into global_data_mart.setup_Schema.pos_transactions_processed (
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
from global_data_mart.setup_Schema.stream_pos_new;

create or replace task global_data_mart.setup_Schema.erp_orders_task
warehouse = compute_wh
schedule = '5 minute'
as
insert into global_data_mart.setup_Schema.erp_orders_processed (
    order_id, order_date, store_id, store_city, supplier_id, supplier_name,
    supplier_city, product_sku, category, quantity_ordered, quantity_received,
    unit_cost, total_cost, order_status, expected_delivery, actual_delivery,
    warehouse_id, lead_time_days, is_late
)
select
    order_id, order_date, store_id, store_city, supplier_id, supplier_name,
    supplier_city, product_sku, category, quantity_ordered, quantity_received,
    unit_cost, total_cost, order_status, expected_delivery, actual_delivery,
    warehouse_id, lead_time_days, is_late
from global_data_mart.setup_Schema.stream_erp_orders;

create or replace task global_data_mart.setup_Schema.erp_inventory_task
warehouse = compute_wh
schedule = '5 minute'
as
insert into global_data_mart.setup_Schema.erp_inventory_processed (
    snapshot_date, store_id, warehouse_id, product_sku, category,
    quantity_on_hand, reorder_level, max_stock_level, last_received_date
)
select
    snapshot_date, store_id, warehouse_id, product_sku, category,
    quantity_on_hand, reorder_level, max_stock_level, last_received_date
from global_data_mart.setup_Schema.stream_erp_inventory;


create or replace task global_data_mart.setup_Schema.iot_task
warehouse = compute_wh
schedule = '5 minute'
as
insert into global_data_mart.setup_Schema.iot_events_processed (data)
select data from global_data_mart.setup_Schema.stream_iot_events;

alter task global_data_mart.setup_Schema.pos_task suspend;
alter task global_data_mart.setup_Schema.erp_orders_task suspend;
alter task global_data_mart.setup_Schema.erp_inventory_task suspend;
alter task global_data_mart.setup_Schema.iot_task suspend;

alter task global_data_mart.setup_Schema.pos_task resume;
alter task global_data_mart.setup_Schema.erp_orders_task resume;
alter task global_data_mart.setup_Schema.erp_inventory_task resume;
alter task global_data_mart.setup_Schema.iot_task resume;

-- ============ checks ============

select *
from global_data_mart.setup_Schema.pos_transactions
where load_ts >= dateadd(minute, -5, current_timestamp())
limit 100;

describe table global_data_mart.setup_Schema.erp_orders;
describe table global_data_mart.setup_Schema.erp_inventory;
describe table global_data_mart.setup_Schema.iot_events_raw;

select * from global_data_mart.setup_Schema.pos_transactions;
select * from global_data_mart.setup_Schema.erp_orders;
select * from global_data_mart.setup_Schema.erp_inventory;
select * from global_data_mart.setup_Schema.iot_events;



-- stores with high sales
select p.store_id, round(sum(p.total_amount), 2) as revenue, round(avg(e.battery_pct)) as avg_sensor_battery
from global_data_mart.setup_Schema.pos_transactions p
join global_data_mart.setup_Schema.iot_events e on p.store_id = e.store_id
group by p.store_id
order by revenue desc;



select *
from table(information_schema.copy_history(
    table_name => 'global_data_mart.setup_Schema.pos_transactions',
    start_time => dateadd(hours, -24, current_timestamp())
));

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


-------------------------------------------------------------------------------------------------------------------
--========= TASK CHAIN ===================-----

create or replace task global_data_mart.setup_schema.task_refresh_dim_region
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_sales_buffer
as
create or replace table global_data_mart.setup_schema.dim_region as
select distinct store_region as region_name
from global_data_mart.setup_schema.pos_transactions;

create or replace task global_data_mart.setup_schema.task_refresh_dim_city
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_dim_region
as
create or replace table global_data_mart.setup_schema.dim_city as
select distinct store_city as city_name, store_region as region_name
from global_data_mart.setup_schema.pos_transactions;

create or replace task global_data_mart.setup_schema.task_refresh_dim_store
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_dim_city
as
create or replace table global_data_mart.setup_schema.dim_store as
select distinct store_id, store_name, store_city as city_name
from global_data_mart.setup_schema.pos_transactions;

create or replace task global_data_mart.setup_schema.task_refresh_dim_category
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_dim_store
as
create or replace table global_data_mart.setup_schema.dim_category as
select distinct category as category_name
from global_data_mart.setup_schema.pos_transactions;

create or replace task global_data_mart.setup_schema.task_refresh_dim_subcategory
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_dim_category
as
create or replace table global_data_mart.setup_schema.dim_subcategory as
select distinct subcategory as subcategory_name, category as category_name
from global_data_mart.setup_schema.pos_transactions;

create or replace task global_data_mart.setup_schema.task_refresh_dim_product
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_dim_subcategory
as
create or replace table global_data_mart.setup_schema.dim_product as
select distinct product_sku, product_name, subcategory as subcategory_name
from global_data_mart.setup_schema.pos_transactions;

create or replace task global_data_mart.setup_schema.task_refresh_dim_year
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_dim_product
as
create or replace table global_data_mart.setup_schema.dim_year as
select distinct year(transaction_date) as year
from global_data_mart.setup_schema.pos_transactions;

create or replace task global_data_mart.setup_schema.task_refresh_dim_month
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_dim_year
as
create or replace table global_data_mart.setup_schema.dim_month as
select distinct month(transaction_date) as month, year(transaction_date) as year
from global_data_mart.setup_schema.pos_transactions;

create or replace task global_data_mart.setup_schema.task_refresh_dim_date
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_dim_month
as
create or replace table global_data_mart.setup_schema.dim_date as
select distinct transaction_date as date_key,
    day(transaction_date) as day,
    month(transaction_date) as month
from global_data_mart.setup_schema.pos_transactions;

create or replace task global_data_mart.setup_schema.task_refresh_fct_daily_sales
warehouse = compute_wh
after global_data_mart.setup_schema.task_refresh_dim_date
as
create or replace table global_data_mart.setup_schema.fct_daily_sales as
select transaction_date as date_key, store_id, product_sku, category,
    count(*) as total_transactions,
    sum(quantity) as total_quantity,
    sum(total_amount) as total_revenue
from global_data_mart.setup_schema.pos_transactions
group by transaction_date, store_id, product_sku, category;

-------------------------------------------------------------------------------------------------------------------
--========= TASK: SCD2 Store Update  ===================-----

create or replace task global_data_mart.setup_schema.task_scd2_store_update
warehouse = compute_wh
schedule = '5 minute'
as
merge into global_data_mart.setup_schema.dim_store_scd2 as tgt
using global_data_mart.setup_schema.stg_store_updates as src
    on tgt.store_id = src.store_id and tgt.is_current = true
when matched and tgt.city_name != src.store_city then
    update set
        tgt.end_date = current_date(),
        tgt.is_current = false;

-------------------------------------------------------------------------------------------------------------------
--========= Resume all new tasks  ===================-----

alter task global_data_mart.setup_schema.task_refresh_fct_daily_sales resume;
alter task global_data_mart.setup_schema.task_refresh_dim_date resume;
alter task global_data_mart.setup_schema.task_refresh_dim_month resume;
alter task global_data_mart.setup_schema.task_refresh_dim_year resume;
alter task global_data_mart.setup_schema.task_refresh_dim_product resume;
alter task global_data_mart.setup_schema.task_refresh_dim_subcategory resume;
alter task global_data_mart.setup_schema.task_refresh_dim_category resume;
alter task global_data_mart.setup_schema.task_refresh_dim_store resume;
alter task global_data_mart.setup_schema.task_refresh_dim_city resume;
alter task global_data_mart.setup_schema.task_refresh_dim_region resume;
alter task global_data_mart.setup_schema.task_refresh_sales_buffer resume;
alter task global_data_mart.setup_schema.task_scd2_store_update resume;
alter task global_data_mart.setup_schema.task_pos_incremental resume;

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
-- 3. Gross Margin view 
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

-------------------------------------------------------------------------------------------------------------------
--========= SCD TYPE - 2 ===================-----
 
-- DIMENSION TABLE 
 
create or replace table global_data_mart.setup_schema.dim_store_scd2 (
    store_key       int autoincrement primary key,  -- surrogate key
    store_id        string,                        
    store_name      string,
    city_name       string,
    effective_date  date,
    end_date        date,
    is_current      boolean
);
 
-- ============================================================

 
insert into global_data_mart.setup_schema.dim_store_scd2
    (store_id, store_name, city_name, effective_date, end_date, is_current)
select distinct store_id, store_name, store_city,
    '2026-01-01'::date as effective_date,
    null as end_date,
    true as is_current
from global_data_mart.setup_schema.pos_transactions;
 
select * from global_data_mart.setup_schema.dim_store_scd2 order by store_id;
 

-- ============================================================
 
create or replace table global_data_mart.setup_schema.stg_store_updates (
    store_id    string,
    store_name  string,
    store_city  string
);
 

merge into global_data_mart.setup_schema.dim_store_scd2 as tgt
using global_data_mart.setup_schema.stg_store_updates as src
    on tgt.store_id = src.store_id and tgt.is_current = true
when matched and tgt.city_name != src.store_city then
    update set
        tgt.end_date = current_date(),
        tgt.is_current = false;
 
insert into global_data_mart.setup_schema.dim_store_scd2
    (store_id, store_name, city_name, effective_date, end_date, is_current)
select src.store_id, src.store_name, src.store_city,
    current_date() as effective_date,
    null as end_date,
    true as is_current
from global_data_mart.setup_schema.stg_store_updates src
join global_data_mart.setup_schema.dim_store_scd2 tgt
    on src.store_id = tgt.store_id
where tgt.is_current = false
  and tgt.end_date = current_date();
 

 
select * from global_data_mart.setup_schema.dim_store_scd2
where store_id = 'STR_001'
order by effective_date;
 
select * from global_data_mart.setup_schema.dim_store_scd2
order by store_id, effective_date;
 
