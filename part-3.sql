-- Global Data Mart queries, views, and dimensional model tables
-- Co-authored with CoCo
use database global_data_mart;

create or replace table global_data_mart.setup_schema.test_1
(
    transaction_id string primary key,
    store_id int,
    store_name char(50),
    time_transaction timestamp,
    quantity int,
    unit_price float,
    total_revenue float,
    total_revenue_discount float,
    "year" int,
    "month" int
);

select transaction_id, store_name, to_timestamp(transaction_date || ' ' || transaction_time) as time_transaction,
    quantity, unit_price, quantity * unit_price as revenue,
    quantity * unit_price * (1 - discount_pct/100) as discounted_price,
    year(transaction_date) as year_transaction, month(transaction_date) as month_transaction
from global_data_mart.setup_schema.pos_transactions;

select greatest(-1,0) < 100;

select count(*) from global_data_mart.setup_schema.pos_transactions;

-- -------------------------------------------

create or replace table global_data_mart.setup_schema.daily_store_category_revenue as
select transaction_date, store_id, store_name, store_city, store_region, category,
    sum(total_amount) as total_revenue, sum(quantity) as units_sold,
    count(*) as total_transactions, count(distinct transaction_id) as unique_transactions,
    round(sum(total_amount) / nullif(count(*), 0), 2) as avg_transaction_size
from global_data_mart.setup_schema.pos_transactions
group by transaction_date, store_id, store_name, store_city, store_region, category;

select * from global_data_mart.setup_schema.daily_store_category_revenue;

select transaction_date, store_id, store_name, store_city, store_region, category,
    sum(total_amount) as total_revenue, sum(quantity) as units_sold,
    count(*) as total_transactions, count(distinct transaction_id) as unique_transactions,
    round(sum(total_amount) / nullif(count(*), 0), 2) as avg_transaction_size
from global_data_mart.setup_schema.pos_transactions
group by transaction_date, store_id, store_name, store_city, store_region, category
order by transaction_date, store_id, category;

--------------------------------------------------------------------------------------
-- NOTE: iot_events already exists as a TABLE (built in the ingestion script).
-- Removed the duplicate "create or replace view iot_events" block that was here —
-- it conflicted with the existing table and would fail with
-- "IOT_EVENTS already exists as TABLE".
--------------------------------------------------------------------------------------

create or replace view global_data_mart.setup_schema.store_daily_revenue_sensors as
select p.store_id, p.store_name, p.transaction_date,
    sum(p.total_amount) as total_revenue,
    avg(s.battery_pct) as avg_battery_pct,
    avg(s.signal_rssi) as avg_signal_rssi,
    case
        when avg(s.battery_pct) is null then 'no sensor data'
        else 'has sensor data'
    end as sensor_value_flag
from global_data_mart.setup_schema.pos_transactions p
left join global_data_mart.setup_schema.iot_events s
    on p.store_id = s.store_id
   and p.transaction_date = date(s.event_timestamp)
group by p.store_id, p.store_name, p.transaction_date;

select * from global_data_mart.setup_schema.store_daily_revenue_sensors
order by store_id, transaction_date;

-- ============ separate weight_kg and temp_c averages ============
select table_catalog, table_schema, table_name, row_count
from global_data_mart.information_schema.tables
where table_name ilike 'pos_transactions%';

select p.store_id, p.store_name, p.transaction_date,
    sum(p.total_amount) as total_revenue,
    avg(s.battery_pct) as avg_battery_pct,
    avg(s.signal_rssi) as avg_signal_rssi,
    min(s.battery_pct) as min_battery_pct,
    max(s.battery_pct) as max_battery_pct
from global_data_mart.setup_schema.pos_transactions p
left join global_data_mart.setup_schema.iot_events s
    on p.store_id = s.store_id
   and p.transaction_date = date(s.event_timestamp)
group by p.store_id, p.store_name, p.transaction_date;
-- order by p.store_id, p.transaction_date;

-- FACT AND DIMENSION TABLE

-- ============ SNOWFLAKE SCHEMA — normalized dimensions ============

-- dim_region (top of the store hierarchy)
create or replace table global_data_mart.setup_schema.dim_region as
select distinct store_region as region_name
from global_data_mart.setup_schema.pos_transactions;

-- dim_city (references region)
create or replace table global_data_mart.setup_schema.dim_city as
select distinct store_city as city_name, store_region as region_name
from global_data_mart.setup_schema.pos_transactions;

-- dim_store (references city, NOT region directly — that's the snowflake part)
create or replace table global_data_mart.setup_schema.dim_store as
select distinct store_id, store_name, store_city as city_name
from global_data_mart.setup_schema.pos_transactions;

-- dim_category (top of the product hierarchy)
create or replace table global_data_mart.setup_schema.dim_category as
select distinct category as category_name
from global_data_mart.setup_schema.pos_transactions;

-- dim_subcategory (references category)
create or replace table global_data_mart.setup_schema.dim_subcategory as
select distinct subcategory as subcategory_name, category as category_name
from global_data_mart.setup_schema.pos_transactions;

-- dim_product (references subcategory, NOT category directly)
create or replace table global_data_mart.setup_schema.dim_product as
select distinct product_sku, product_name, subcategory as subcategory_name
from global_data_mart.setup_schema.pos_transactions;

-- dim_year (top of the date hierarchy)
create or replace table global_data_mart.setup_schema.dim_year as
select distinct year(transaction_date) as year
from global_data_mart.setup_schema.pos_transactions;

-- dim_month (references year)
create or replace table global_data_mart.setup_schema.dim_month as
select distinct month(transaction_date) as month, year(transaction_date) as year
from global_data_mart.setup_schema.pos_transactions;

-- dim_date (references month, NOT year directly)
create or replace table global_data_mart.setup_schema.dim_date as
select distinct transaction_date as date_key,
    day(transaction_date) as day,
    month(transaction_date) as month
from global_data_mart.setup_schema.pos_transactions;

-- fct_daily_sales (unchanged — grain and measures stay the same)
create or replace table global_data_mart.setup_schema.fct_daily_sales as
select transaction_date as date_key, store_id, product_sku, category,
    count(*) as total_transactions,
    sum(quantity) as total_quantity,
    sum(total_amount) as total_revenue
from global_data_mart.setup_schema.pos_transactions
group by transaction_date, store_id, product_sku, category;
