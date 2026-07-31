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

-- Flatten the raw VARIANT IoT data into a relational view
create or replace view global_data_mart.setup_schema.iot_events as
select
    data:event_id::string       as event_id,
    data:device_id::string      as device_id,
    data:store_id::string       as store_id,
    data:store_name::string     as store_name,
    data:event_type::string     as event_type,
    data:timestamp::timestamp   as event_timestamp,
    data:metadata.battery_pct::number   as battery_pct,
    data:metadata.signal_rssi::number   as signal_rssi,
    data:metadata.firmware::string      as firmware,
    data:metadata.store_floor::number   as store_floor,
    load_ts
from global_data_mart.setup_schema.iot_events_raw;

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

-- dim_date
create or replace table dim_date as
select distinct transaction_date as date_key,
    year(transaction_date) as year,
    month(transaction_date) as month,
    day(transaction_date) as day from staging_schema.stg_pos_transactions;

-- dim_store

create or replace table dim_store as
select distinct store_id,store_name,store_city,store_region
from staging_schema.stg_pos_transactions;

-- dim_product

create or replace table dim_product as
select distinct product_sku, product_name, category,subcategoryfrom staging_schema.stg_pos_transactions;

-- fct_daily_sales

create or replace table fct_daily_sales as
select transaction_date as date_key,store_id,product_sku,category,
    count(*) as total_transactions,sum(quantity) as total_quantity,
    sum(total_amount) as total_revenue from staging_schema.stg_pos_transactions
group by transaction_date, store_id, product_sku, category;
