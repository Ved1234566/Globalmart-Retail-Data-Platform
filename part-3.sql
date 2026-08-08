-- Global Data Mart queries, views, and dimensional  tables

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

select DATEADD(day , row_number()over(order by null)-1 ,'2023-01-01') as generator_date
from table(generator(rowcount=>5));

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

-- dim_category
create or replace table global_data_mart.setup_schema.dim_category as
select distinct category as category_name
from global_data_mart.setup_schema.pos_transactions;

-- dim_subcategory 
create or replace table global_data_mart.setup_schema.dim_subcategory as
select distinct subcategory as subcategory_name, category as category_name
from global_data_mart.setup_schema.pos_transactions;

-- dim_product 
create or replace table global_data_mart.setup_schema.dim_product as
select distinct product_sku, product_name, subcategory as subcategory_name
from global_data_mart.setup_schema.pos_transactions;

-- dim_year 
create or replace table global_data_mart.setup_schema.dim_year as
select distinct year(transaction_date) as year
from global_data_mart.setup_schema.pos_transactions;

-- dim_month 
create or replace table global_data_mart.setup_schema.dim_month as
select distinct month(transaction_date) as month, year(transaction_date) as year
from global_data_mart.setup_schema.pos_transactions;

-- dim_date 
create or replace table global_data_mart.setup_schema.dim_date as
select distinct transaction_date as date_key,
    day(transaction_date) as day,
    month(transaction_date) as month
from global_data_mart.setup_schema.pos_transactions;

-- fct_daily_sales
create or replace table global_data_mart.setup_schema.fct_daily_sales as
select transaction_date as date_key, store_id, product_sku, category,
    count(*) as total_transactions,
    sum(quantity) as total_quantity,
    sum(total_amount) as total_revenue
from global_data_mart.setup_schema.pos_transactions
group by transaction_date, store_id, product_sku, category;



-- ============================================================
-- fct_gross_margin — Gold layer fact table
-- ============================================================

create or replace table global_data_mart.mart.fct_gross_margin (
    store_id            varchar(10),                              
    store_name          varchar(100),
    store_city          varchar(50),
    category            varchar(50),
    total_revenue       float,                                    
    total_cost          float,
    gross_profit        float,                                   
    gross_margin_pct    float,                                    
    total_units_sold    int,                                  
    total_orders        int, 
    updated_at          timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30;


insert into global_data_mart.mart.fct_gross_margin (
    store_id, store_name, store_city, category,
    total_revenue, total_cost, gross_profit, gross_margin_pct,
    total_units_sold, total_orders
)
with pos_agg as (
    select store_id, store_name, store_city, category,
        sum(total_amount) as total_revenue,
        sum(quantity) as total_units_sold,
        count(distinct transaction_id) as total_orders
    from global_data_mart.setup_schema.pos_transactions
    group by store_id, store_name, store_city, category
),
erp_agg as (
    select store_id, category,
        sum(total_cost) AS total_cost
    from global_data_mart.setup_schema.erp_orders
    group by store_id, category
)
select
    p.store_id, p.store_name, p.store_city, p.category,
    p.total_revenue,
    e.total_cost,
    p.total_revenue - e.total_cost AS gross_profit,
    round((p.total_revenue - e.total_cost) / NULLIF(p.total_revenue, 0) * 100, 2) AS gross_margin_pct,
    p.total_units_sold,
    p.total_orders
from pos_agg p
left join erp_agg e
    on p.store_id = e.store_id
   and p.category = e.category;


select * from global_data_mart.mart.fct_gross_margin 
order by gross_margin_pct;

-- fact table part - 2 per day data 

create or replace table global_data_mart.mart.fact_daily_sales (
    report_date date , 
    store_id varchar(10),
    store_name varchar(100),
    store_city varchar(50),
    store_region varchar(50),
    category varchar(50),
    total_revenue float,
    total_units int,
    total_transaction int,
    avg_basket float,
    unique_customer int,
    updated_at timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30;

insert into global_data_mart.mart.fact_daily_sales
select transaction_date,
       store_id, store_name, store_city, store_region,
       category,
       round(sum(total_amount), 2) as total_revenue,
       sum(quantity) as total_units,
       count(distinct transaction_id) as total_transactions,
       round(avg(total_amount), 2) as avg_basket,
       count(distinct customer_id) as unique_customer,
       current_timestamp()
from global_data_mart.setup_schema.pos_transactions
group by transaction_date, store_id, store_name, store_city, store_region, category;

select * FROM global_data_mart.mart.fact_daily_sales 
order by  report_date;

-- Fact Table - 3

create or replace table global_data_mart.stg_pos_transaction
fct_gross_margin (
    store_id varchar(10),
    store_name varchar(50),
    store_city varchar(50),
    category varchar(50),
    total_revenue float ,
    total_cost float,
    gross_profit float,
    total_margin_act float,
    total_unit_sold int,
    total_orders int,
    updated_at timestamp_ntz default current_timestamp()
);

-- CTE with join 

with pos_agg as (
    select store_id, store_name, store_city,
        category,
        round(sum(total_amount), 2) as total_revenue,
        sum(quantity)                as total_units_sold,
        round(avg(unit_price), 2)    as avg_selling_price
    from global_data_mart.setup_schema.pos_transactions
    where total_amount > 0
    group by store_id, store_name, store_city, category
),
erp_units as (
    select store_id, category,
        round(avg(unit_cost), 4) as avg_unit_sold,
        count(distinct order_id) as total_orders
    from global_data_mart.setup_schema.erp_orders
    where unit_cost > 0
    group by store_id, category
)
select
    p.store_id, p.store_name, p.store_city, p.category,
    p.total_revenue, p.total_units_sold, p.avg_selling_price,
    e.avg_unit_sold, e.total_orders
from pos_agg p
left join erp_units e
    on p.store_id = e.store_id
   and p.category = e.category;
