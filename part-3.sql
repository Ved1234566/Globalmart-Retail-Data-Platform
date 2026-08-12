use database global_data_mart;

create schema if not exists global_data_mart.mart;

create or replace table global_data_mart.setup_schema.test_1
(
    transaction_id string primary key,
    store_id int,
    store_name varchar(50),
    time_transaction timestamp,
    quantity int,
    unit_price float,
    total_revenue float,
    total_revenue_discount float,
    "year" int,
    "month" int
);


select
    transaction_id,
    store_name,
    to_timestamp(transaction_date || ' ' || transaction_time) as time_transaction,
    quantity,
    unit_price,
    quantity * unit_price as revenue,
    quantity * unit_price * (1 - discount_pct / 100) as discounted_price,
    year(transaction_date) as year_transaction,
    month(transaction_date) as month_transaction
from global_data_mart.setup_schema.pos_transactions;


select greatest(-1, 0) < 100;


select
    dateadd(day, row_number() over(order by null) - 1, '2023-01-01') as generator_date
from table(generator(rowcount => 5));


select count(*)
from global_data_mart.setup_schema.pos_transactions;


-- ============================================================
-- DAILY STORE CATEGORY REVENUE
-- ============================================================

create or replace table global_data_mart.setup_schema.daily_store_category_revenue as
select
    transaction_date,
    store_id,
    store_name,
    store_city,
    store_region,
    category,
    sum(total_amount) as total_revenue,
    sum(quantity) as units_sold,
    count(*) as total_transactions,
    count(distinct transaction_id) as unique_transactions,
    round(sum(total_amount) / nullif(count(*), 0), 2) as avg_transaction_size
from global_data_mart.setup_schema.pos_transactions
group by
    transaction_date,
    store_id,
    store_name,
    store_city,
    store_region,
    category;


select *
from global_data_mart.setup_schema.daily_store_category_revenue;


-- ============================================================
-- IOT EVENTS
-- ============================================================

create or replace table global_data_mart.setup_schema.iot_events as
with base as
(
    select
        data:event_id::string as event_id,
        data:event_type::string as event_type,
        data:store_id::string as store_id,
        data:store_name::string as store_name,
        data:timestamp::timestamp as event_timestamp,
        data:device_id::string as device_id,
        data:metadata:firmware::string as firmware,
        data:metadata:battery_pct::int as battery_pct,
        data:metadata:signal_rssi::int as signal_rssi,
        data:metadata:store_floor::int as store_floor,
        data:readings as readings
    from global_data_mart.setup_schema.iot_events_raw
),

flattened as
(
    select
        b.event_id,
        r.value:sensor::string as sensor_name,
        r.value:value::float as sensor_value
    from base b,
    lateral flatten(input => b.readings) r
)

select b.event_id,b.event_type,b.store_id,b.store_name,b.event_timestamp,
    b.device_id,b.firmware,b.battery_pct,b.signal_rssi,b.store_floor,

    max(case
        when f.sensor_name = 'temp_c'
        then f.sensor_value
    end) as temp_c,

    max(case
        when f.sensor_name = 'weight_kg'
        then f.sensor_value
    end) as weight_kg,

    max(case
        when f.sensor_name = 'footfall'
        then f.sensor_value
    end) as footfall,

    max(case
        when f.sensor_name = 'occupancy_pct'
        then f.sensor_value
    end) as occupancy_pct,

    max(case
        when f.sensor_name = 'humidity_pct'
        then f.sensor_value
    end) as humidity_pct,

    max(case
        when f.sensor_name = 'power_kw'
        then f.sensor_value
    end) as power_kw,

    max(case
        when f.sensor_name = 'queue_length'
        then f.sensor_value
    end) as queue_length,

    max(case
        when f.sensor_name = 'voltage'
        then f.sensor_value
    end) as voltage

from base b
left join flattened f
    on b.event_id = f.event_id

group by b.event_id, b.event_type,b.store_id,b.store_name,b.event_timestamp,
    b.device_id,b.firmware,b.battery_pct,b.signal_rssi,b.store_floor;


select * from global_data_mart.setup_schema.iot_events;


-- ============================================================
-- STORE DAILY REVENUE + SENSOR
-- ============================================================

create or replace view global_data_mart.setup_schema.store_daily_revenue_sensors as
select p.store_id,p.store_name,p.transaction_date,
    sum(p.total_amount) as total_revenue,
    avg(s.battery_pct) as avg_battery_pct,
    avg(s.signal_rssi) as avg_signal_rssi,

    case
        when avg(s.battery_pct) is null
            then 'no sensor data'
        else 'has sensor data'
    end as sensor_value_flag

from global_data_mart.setup_schema.pos_transactions p

left join global_data_mart.setup_schema.iot_events s
    on p.store_id = s.store_id
    and p.transaction_date = date(s.event_timestamp)

group by
    p.store_id,
    p.store_name,
    p.transaction_date;


select *
from global_data_mart.setup_schema.store_daily_revenue_sensors
order by store_id, transaction_date;


-- ============================================================
-- DIMENSION TABLES
-- ============================================================

create or replace table global_data_mart.setup_schema.dim_region as
select distinct store_region as region_name
from global_data_mart.setup_schema.pos_transactions;


create or replace table global_data_mart.setup_schema.dim_city as
select distinct store_city as city_name,store_region as region_name
from global_data_mart.setup_schema.pos_transactions;


create or replace table global_data_mart.setup_schema.dim_store as
select distinct store_id,store_name,store_city as city_name
from global_data_mart.setup_schema.pos_transactions;


create or replace table global_data_mart.setup_schema.dim_category as
select distinct category as category_name
from global_data_mart.setup_schema.pos_transactions;


create or replace table global_data_mart.setup_schema.dim_subcategory as
select distinct subcategory as subcategory_name, category as category_name
from global_data_mart.setup_schema.pos_transactions;


create or replace table global_data_mart.setup_schema.dim_product as
select distinct product_sku,product_name,subcategory as subcategory_name
from global_data_mart.setup_schema.pos_transactions;


create or replace table global_data_mart.setup_schema.dim_year as
select distinct year(transaction_date) as year
from global_data_mart.setup_schema.pos_transactions;


create or replace table global_data_mart.setup_schema.dim_month as
select distinct
    month(transaction_date) as month,
    year(transaction_date) as year
from global_data_mart.setup_schema.pos_transactions;


create or replace table global_data_mart.setup_schema.dim_date as
select distinct transaction_date as date_key,
    day(transaction_date) as day,
    month(transaction_date) as month,
    year(transaction_date) as year
from global_data_mart.setup_schema.pos_transactions;


-- ============================================================
-- FACT DAILY SALES
-- ============================================================

create or replace table global_data_mart.setup_schema.fct_daily_sales as
select transaction_date as date_key, store_id,product_sku,category,
    count(*) as total_transactions,
    sum(quantity) as total_quantity,
    sum(total_amount) as total_revenue
from global_data_mart.setup_schema.pos_transactions
group by transaction_date, store_id,product_sku,category;


-- ============================================================
-- FACT GROSS MARGIN
-- ============================================================

create or replace table global_data_mart.mart.fct_gross_margin
(
    store_id varchar(10),
    store_name varchar(100),
    store_city varchar(50),
    category varchar(50),
    total_revenue float,
    total_cost float,
    gross_profit float,
    gross_margin_pct float,
    total_units_sold int,
    total_orders int,
    updated_at timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30;


insert into global_data_mart.mart.fct_gross_margin
(
    store_id,store_name,store_city,category,total_revenue,
    total_cost,gross_profit,gross_margin_pct,total_units_sold,
    total_orders
)

with pos_agg as
(
    select
        store_id,store_name,store_city,category,
        sum(total_amount) as total_revenue,
        sum(quantity) as total_units_sold,
        count(distinct transaction_id) as total_orders
    from global_data_mart.setup_schema.pos_transactions
    group by store_id,store_name,store_city,category
),

erp_agg as
(
    select store_id,category,
        sum(total_cost) as total_cost
    from global_data_mart.setup_schema.erp_orders
    group by
        store_id,
        category
)

select p.store_id, p.store_name,p.store_city,p.category,p.total_revenue,
    coalesce(e.total_cost, 0) as total_cost,

    p.total_revenue - coalesce(e.total_cost, 0) as gross_profit,

    round(
        (p.total_revenue - coalesce(e.total_cost, 0))
        / nullif(p.total_revenue, 0) * 100,
        2
    ) as gross_margin_pct,

    p.total_units_sold,
    p.total_orders

from pos_agg p

left join erp_agg e
    on p.store_id = e.store_id
    and p.category = e.category;


select *
from global_data_mart.mart.fct_gross_margin
order by gross_margin_pct;


-- ============================================================
-- FACT DAILY SALES - DAY LEVEL
-- ============================================================

create or replace table global_data_mart.mart.fact_daily_sales
(
    report_date date,
    store_id varchar(10),
    store_name varchar(100),
    store_city varchar(50),
    store_region varchar(50),
    category varchar(50),
    total_revenue float,
    total_units int,
    total_transactions int,
    avg_basket float,
    unique_customer int,
    updated_at timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30;


insert into global_data_mart.mart.fact_daily_sales
(
    report_date,store_id,store_name,store_city,store_region,
    category,total_revenue,total_units,total_transactions,
    avg_basket,unique_customer
)

select
    transaction_date,
    store_id,store_name,store_city,store_region,category,
    round(sum(total_amount), 2),
    sum(quantity),
    count(distinct transaction_id),
    round(avg(total_amount), 2),
    count(distinct customer_id)

from global_data_mart.setup_schema.pos_transactions

group by transaction_date,store_id,store_name,store_city,store_region,category;


select *from global_data_mart.mart.fact_daily_sales
order by report_date;


-- ============================================================
-- GROSS MARGIN STAGING
-- ============================================================

create or replace table global_data_mart.setup_schema.stg_pos_transaction_gross_margin
(
    store_id varchar(10),
    store_name varchar(50),
    store_city varchar(50),
    category varchar(50),
    total_revenue float,
    total_cost float,
    gross_profit float,
    total_margin_act float,
    total_unit_sold int,
    total_orders int,
    updated_at timestamp_ntz default current_timestamp()
);


insert into global_data_mart.setup_schema.stg_pos_transaction_gross_margin
(
    store_id,store_name,store_city,category,total_revenue,
    total_cost,gross_profit,total_margin_act,total_unit_sold,
    total_orders
)

with pos_agg as
(
    select store_id,store_name,store_city,category,
        round(sum(total_amount), 2) as total_revenue,
        sum(quantity) as total_units_sold
    from global_data_mart.setup_schema.pos_transactions
    where total_amount > 0
    group bystore_id,store_name,store_city,category
),

erp_agg as
(
    select store_id,category,
        sum(unit_cost) as total_cost,
        count(distinct order_id) as total_orders
    from global_data_mart.setup_schema.erp_orders
    where unit_cost > 0
    group by store_id,category
)

select
    p.store_id,p.store_name,p.store_city,p.category,p.total_revenue,
    coalesce(e.total_cost, 0) as total_cost,

    p.total_revenue - coalesce(e.total_cost, 0) as gross_profit,

    round(
        (p.total_revenue - coalesce(e.total_cost, 0))
        / nullif(p.total_revenue, 0) * 100,
        2
    ) as total_margin_act,

    p.total_units_sold,
    coalesce(e.total_orders, 0)

from pos_agg p

left join erp_agg e
    on p.store_id = e.store_id
    and p.category = e.category;


-- ============================================================
-- IOT DAILY FACT
-- ============================================================

create or replace table global_data_mart.mart.fct_store_iot_daily
(
    event_date date,
    store_id varchar(30),
    store_name varchar(100),
    avg_temp_c float,
    max_temp_c float,
    avg_weight_kg float,
    avg_footfall float,
    total_footfall int,
    avg_occupancy_pct float,
    avg_humidity_pct float,
    avg_power_kw float,
    avg_queue_length float,
    avg_voltage float,
    device_count int,
    low_battery_cnt int,
    event_count int,
    updated_at timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30;


insert into global_data_mart.mart.fct_store_iot_daily
(
    event_date,store_id,store_name,avg_temp_c,max_temp_c,avg_weight_kg,
    avg_footfall,total_footfall,avg_occupancy_pct,avg_humidity_pct,avg_power_kw,
    avg_queue_length,avg_voltage,device_count,low_battery_cnt,event_count
)

select
    date(event_timestamp),
    store_id,store_name,
    avg(temp_c),
    max(temp_c),
    avg(weight_kg),
    avg(footfall),
    sum(footfall),
    avg(occupancy_pct),
    avg(humidity_pct),
    avg(power_kw),
    avg(queue_length),
    avg(voltage),
    count(distinct device_id),
    sum(iff(battery_pct < 20, 1, 0)),
    count(*)

from global_data_mart.setup_schema.iot_events

group by date(event_timestamp),store_id,store_name;


-- ============================================================
-- SALES VS IOT
-- ============================================================

create or replace table global_data_mart.mart.fct_sales_vs_iot
(
    report_date date,
    store_id varchar(10),
    store_name varchar(100),
    category varchar(50),

    total_revenue float,
    total_txns int,
    avg_basket float,
    unique_customers int,

    avg_temp_c float,
    max_temp_c float,
    avg_footfall float,
    total_footfall int,
    avg_shelf_weight float,
    avg_power_kw float,
    avg_queue_length float,
    avg_occupancy_pct float,
    device_count int,
    low_battery_cnt int,

    is_temp_breach boolean,
    is_low_stocks boolean,
    operational_status varchar(20),

    updated_at timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30;


insert into global_data_mart.mart.fct_sales_vs_iot
(
    report_date,
    store_id,
    store_name,
    category,

    total_revenue,
    total_txns,
    avg_basket,
    unique_customers,

    avg_temp_c,
    max_temp_c,
    avg_footfall,
    total_footfall,
    avg_shelf_weight,
    avg_power_kw,
    avg_queue_length,
    avg_occupancy_pct,
    device_count,
    low_battery_cnt,

    is_temp_breach,
    is_low_stocks,
    operational_status
)

with pos as
(
    select
        report_date,
        store_id,
        store_name,
        category,

        sum(total_revenue) as total_revenue,
        sum(total_transactions) as total_txns,
        avg(avg_basket) as avg_basket,
        sum(unique_customer) as unique_customers

    from global_data_mart.mart.fact_daily_sales

    group by report_date,store_id,store_name,category
),

iot as
(
    select
        event_date,
        store_id,
        store_name,

        avg(avg_temp_c) as avg_temp_c,
        max(max_temp_c) as max_temp_c,
        avg(avg_weight_kg) as avg_shelf_weight,
        avg(avg_occupancy_pct) as avg_occupancy_pct,
        avg(avg_footfall) as avg_footfall,
        sum(total_footfall) as total_footfall,
        avg(avg_power_kw) as avg_power_kw,
        avg(avg_queue_length) as avg_queue_length,
        sum(device_count) as device_count,
        sum(low_battery_cnt) as low_battery_cnt

    from global_data_mart.mart.fct_store_iot_daily

    group by event_date,store_id,store_name
)

select
    p.report_date,p.store_id,p.store_name,p.category,
    p.total_revenue,p.total_txns,p.avg_basket,p.unique_customers,

    i.avg_temp_c,i.max_temp_c,i.avg_footfall,i.total_footfall,
    i.avg_shelf_weight,i.avg_power_kw,i.avg_queue_length,i.avg_occupancy_pct,
    i.device_count,i.low_battery_cnt,

    case
        when i.avg_temp_c > 25 then true
        else false
    end as is_temp_breach,

    case
        when i.avg_shelf_weight < 5 then true
        else false
    end as is_low_stocks,

    case
        when i.avg_temp_c > 25 then 'TEMP_BREACH'
        when i.avg_shelf_weight < 5 then 'LOW_STOCK'
        when i.avg_occupancy_pct > 80 then 'OVERCROWD'
        else 'NORMAL'
    end as operational_status
    from pos p
left join iot i
    on p.report_date = i.event_date
    and p.store_id = i.store_id
    and p.store_name = i.store_name;

select *
from global_data_mart.mart.fct_sales_vs_iot
order by report_date, store_id, category;


