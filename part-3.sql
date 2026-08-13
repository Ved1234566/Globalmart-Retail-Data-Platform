

use database  global_data_mart;
use schema    global_data_mart.marts;



use global_data_mart;
show schemas;
use schema marts;


create or replace table global_data_mart.marts.dim_store (
    store_id          varchar(10)   not null,   -- pk: str_001 … str_010
    store_name        varchar(100),
    store_city        varchar(50),
    store_region      varchar(50),
    country           varchar(50)   default 'turkey',
    updated_at        timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 20
comment = 'dim: one row per store — master store reference, join key = store_id';

insert into global_data_mart.marts.dim_store (
    store_id, store_name, store_city, store_region
)
select distinct
    store_id,
    store_name,
    store_city,
    store_region
from global_data_mart.staging.stg_pos_transactions
where store_id is not null
order by store_id;

-- verify — expect 10 rows, one per store
select * from global_data_mart.marts.dim_store order by store_id;




create or replace table global_data_mart.marts.dim_product (
    product_sku       varchar(30)   not null,   -- pk
    product_name      varchar(200),
    category          varchar(50),               -- initcap already applied in silver
    subcategory       varchar(30),
    updated_at        timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30
comment = 'dim: one row per product_sku — category hierarchy, join key = product_sku';

insert into global_data_mart.marts.dim_product (
    product_sku, product_name, category, subcategory
)
select distinct
    product_sku,
    product_name,
    category,
    subcategory
from global_data_mart.staging.stg_pos_transactions
where product_sku is not null;

-- verify — one row per unique sku
select count(*) as total_products,
       count(distinct category) as categories
from   global_data_mart.marts.dim_product;

select *  from  global_data_mart.marts.dim_product order by product_sku;;



with date_values as(
select 
    dateadd('day', row_number() over (order by null)-1 , '2023-01-01'::date) as generated_date
from table(generator(rowcount => 365))
)

select *, extract from date_values;

select seq4() as row_id from table(generator(rowcount => 10));


-- seq4  => 4byte ka integer number create krna
-- 5 generate   => generator

select seq4() as row_id; -- 0
select seq4() as row_id from table(generator(rowcount => 5));

select row_number() over( order by null) from table(generator(rowcount => 5));
-- 2026-08-07  , +2

select 
    dateadd(day, row_number() over (order by null)-1 , '2023-01-01') as generated_date
from table(generator(rowcount => 5));






with cte as
(select dateadd( day, seq4() , '2026-08-01') as date_gener from table(generator(rowcount => 365)) )

select date_gener, month(date_gener), extract( dayofweek from date_gener) from cte;
;


select 
    dateadd(day, row_number() over (order by null)-1 , '2023-01-01') as generated_date
from table(generator(rowcount => 30));

create or replace table global_data_mart.marts.dim_date (
    date_key          date          not null,   -- pk: 2023-01-01 … 2023-12-31
    day_of_week       integer,                  -- 1=mon … 7=sun
    day_name          varchar(10),              -- monday … sunday
    day_of_month      integer,
    day_of_year       integer,
    week_of_year      integer,
    month_number      integer,
    month_name        varchar(10),              -- january … december
    quarter           integer,                  -- 1 … 4
    quarter_name      varchar(6),               -- q1 … q4
    year              integer,
    is_weekend        boolean,
    month_year        varchar(8),               -- jan-2023
    quarter_year      varchar(7),               -- q1-2023
    updated_at        timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30
comment = 'dim: calendar date spine — join key = date_key, used for all time filters';

-- generate one row per date between first and last transaction
insert into global_data_mart.marts.dim_date (
    date_key, day_of_week, day_name, day_of_month, day_of_year,
    week_of_year, month_number, month_name, quarter, quarter_name,
    year, is_weekend, month_year, quarter_year
)
with date_range as (
    select
        min(date(transaction_ts)) as start_date,
        max(date(transaction_ts)) as end_date
    from global_data_mart.staging.stg_pos_transactions
),
row_nums as (
    select row_number() over (order by seq4()) - 1 as n
    from table(generator(rowcount => 400))
),
date_spine as (
    select dateadd('day', r.n, dr.start_date) as d
    from   row_nums    r
    cross join date_range dr
    where  dateadd('day', r.n, dr.start_date) <= dr.end_date
)
select
    d,
    dayofweekiso(d),
    dayname(d),
    day(d),
    dayofyear(d),
    weekofyear(d),
    month(d),
    monthname(d),
    quarter(d),
    'q' || quarter(d),
    year(d),
    dayofweekiso(d) in (6, 7),
    to_char(d, 'mon-yyyy'),
    'q' || quarter(d) || '-' || year(d)
from date_spine
order by d;

-- verify — one row per day in your date range
select
    min(date_key) as first_date,
    max(date_key) as last_date,
    count(*)      as total_days
from global_data_mart.marts.dim_date;


select * from  global_data_mart.marts.dim_date;


create or replace table global_data_mart.marts.dim_supplier (
    supplier_id       varchar(10)   not null,   -- pk
    supplier_name     varchar(100),
    supplier_city     varchar(50),
    updated_at        timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30
comment = 'dim: one row per supplier — from erp parquet, join key = supplier_id';

insert into global_data_mart.marts.dim_supplier (
    supplier_id, supplier_name, supplier_city
)
select distinct
    supplier_id,
    supplier_name,
    supplier_city
from global_data_mart.staging.stg_erp_orders
where supplier_id is not null;

-- verify
select * from global_data_mart.marts.dim_supplier order by supplier_id;




create or replace table global_data_mart.marts.fct_daily_sales (
    report_date       date          not null,   -- fk → dim_date.date_key
    store_id          varchar(10),              -- fk → dim_store.store_id
    store_name        varchar(100),
    store_city        varchar(50),
    store_region      varchar(50),
    category          varchar(50),              -- fk → dim_product.category
    total_revenue     float,                    -- sum(line_total)
    total_units       integer,                  -- sum(quantity)
    total_txns        integer,                  -- count distinct transaction_id
    avg_basket        float,                    -- avg(line_total) per transaction
    unique_customers  integer,                  -- count distinct customer_id
    updated_at        timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30
comment = 'fact: pos daily sales — grain = date+store+category. source: stg_pos_transactions';

insert into global_data_mart.marts.fct_daily_sales
select
    date(transaction_ts)              as report_date,
    store_id,
    store_name,
    store_city,
    store_region,
    category,
    round(sum(line_total), 2)         as total_revenue,
    sum(quantity)                     as total_units,
    count(distinct transaction_id)    as total_txns,
    round(avg(line_total), 2)         as avg_basket,
    count(distinct customer_id)       as unique_customers,
    current_timestamp()
from   global_data_mart.staging.stg_pos_transactions
where  transaction_id is not null
  and  line_total     > 0
group by
    date(transaction_ts), store_id, store_name,
    store_city, store_region, category;


select * from  global_data_mart.marts.fct_daily_sales order by report_date, store_id;

-- verify
select
    count(*)                  as total_rows,
    count(distinct store_id)  as stores,
    count(distinct category)  as categories,
    min(report_date)          as first_date,
    max(report_date)          as last_date,
    round(sum(total_revenue),2) as grand_total_revenue
from global_data_mart.marts.fct_daily_sales;




create or replace table global_data_mart.marts.fct_gross_margin (
    store_id          varchar(10),              -- fk → dim_store.store_id
    store_name        varchar(100),
    store_city        varchar(50),
    category          varchar(50),
    total_revenue     float,                    -- from pos line_total sum
    total_cost        float,                    -- from erp total_cost sum
    gross_profit      float,                    -- revenue − cost
    gross_margin_pct  float,                    -- (revenue−cost)/revenue × 100
    total_units_sold  integer,                  -- from pos quantity sum
    total_orders      integer,                  -- count distinct erp order_id
    updated_at        timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30
comment = 'fact: gross margin — pos revenue join erp cost on store_id+category. grain=store+category';


*/
select * from global_data_mart.staging.stg_pos_transactions;
select * from global_data_mart.staging.stg_erp_orders;


select * from global_data_mart.marts.fct_gross_margin;
insert into global_data_mart.marts.fct_gross_margin
with

-- step 1: aggregate pos to one row per store + category
pos_agg as (
    select
        store_id,
        store_name,
        store_city,
        category,
        round(sum(line_total), 2)      as total_revenue,
        sum(quantity)                    as total_units_sold
    from global_data_mart.staging.stg_pos_transactions
    where line_total > 0
    group by
        store_id, store_name, store_city, category
),

-- step 2: aggregate erp to one row per store + category
erp_agg as (
    select
        store_id,
        category,
        round(sum(total_cost), 2)       as total_cost,
        count(distinct order_id)          as total_orders
    from global_data_mart.staging.stg_erp_orders
    group by store_id, category
)

-- step 3: join the two aggregated results — 1 row joins to 1 row, no fan-out
select
    p.store_id,
    p.store_name,
    p.store_city,
    p.category,
    p.total_revenue,
    coalesce(e.total_cost, 0)                           as total_cost,
    round(p.total_revenue - coalesce(e.total_cost, 0), 2) as gross_profit,
    round(
        (p.total_revenue - coalesce(e.total_cost, 0))
        / nullif(p.total_revenue, 0) * 100
    , 1)                                                   as gross_margin_pct,
    p.total_units_sold,
    coalesce(e.total_orders, 0)                            as total_orders,
    current_timestamp()
from       pos_agg  p
left join  erp_agg  e
        on  p.store_id = e.store_id
       and  p.category = e.category;

-- verify — gross_profit must be positive for all rows
select
    category,
    round(sum(total_revenue), 2)   as total_revenue,
    round(sum(total_cost), 2)      as total_cost,
    round(sum(gross_profit), 2)    as gross_profit,
    round(avg(gross_margin_pct), 1) as avg_margin_pct
from   global_data_mart.marts.fct_gross_margin
group by category
order by avg_margin_pct desc;
-- insert into global_data_mart.marts.fct_gross_margin
-- select
--     p.store_id,
--     p.store_name,
--     p.store_city,
--     p.category,
--     round(sum(p.line_total), 2)                         as total_revenue,
--     round(sum(e.total_cost), 2)                         as total_cost,
--     round(sum(p.line_total) - sum(e.total_cost), 2)    as gross_profit,
--     round(
--         (sum(p.line_total) - sum(e.total_cost))
--         / nullif(sum(p.line_total), 0) * 100
--     , 1)                                                as gross_margin_pct,
--     sum(p.quantity)                                     as total_units_sold,
--     count(distinct e.order_id)                          as total_orders,
--     current_timestamp()
-- from      global_data_mart.staging.stg_pos_transactions  p
-- left join global_data_mart.staging.stg_erp_orders        e
--        on  p.store_id = e.store_id
--       and  p.category = e.category
-- group by
--     p.store_id, p.store_name, p.store_city, p.category;

-- -- verify — see margin by category
-- select
--     category,
--     round(avg(gross_margin_pct), 1) as avg_margin_pct,
--     round(sum(total_revenue), 2)    as total_revenue,
--     round(sum(total_cost), 2)       as total_cost,
--     round(sum(gross_profit), 2)     as total_profit
-- from   global_data_mart.marts.fct_gross_margin
-- group by category
-- order by avg_margin_pct desc;


select * from global_data_mart.marts.fct_gross_margin;

select * from global_data_mart.staging.stg_erp_orders order by store_id;

-- ── truncate first to clear the negative-margin data ──────────
truncate table global_data_mart.marts.fct_gross_margin;

select
        store_id,
        store_name,
        store_city,
        category,
        round(sum(line_total), 2)   as total_revenue,
        sum(quantity)               as total_units_sold,
        round(avg(unit_price), 2)   as avg_selling_price
    from  global_data_mart.staging.stg_pos_transactions
    where line_total > 0
    group by store_id, store_name, store_city, category;


select * from global_data_mart.marts.fct_gross_margin;


insert into global_data_mart.marts.fct_gross_margin
with

-- step 1: pos aggregated per store + category
pos_agg as (
    select
        store_id,
        store_name,
        store_city,
        category,
        round(sum(line_total), 2)   as total_revenue,
        sum(quantity)               as total_units_sold,
        round(avg(unit_price), 2)   as avg_selling_price
    from  global_data_mart.staging.stg_pos_transactions
    where line_total > 0
    group by store_id, store_name, store_city, category
),

-- step 2: erp avg unit_cost per store + category
-- avg(unit_cost) = average wholesale cost per unit for this store+category
-- we multiply this by pos quantity_sold to get cost of goods sold only
erp_unit as (
    select
        store_id,
        category,
        round(avg(unit_cost), 4)        as avg_unit_cost,
        count(distinct order_id)         as total_orders
    from  global_data_mart.staging.stg_erp_orders
    where unit_cost > 0
    group by store_id, category
)

-- step 3: join and compute correct margin
select
    p.store_id,
    p.store_name,
    p.store_city,
    p.category,

    -- revenue = what customers paid
    p.total_revenue,

    -- cost = wholesale cost × units sold (not total_cost from erp)
    round(coalesce(e.avg_unit_cost, 0)
          * p.total_units_sold, 2)                  as total_cost,

    -- gross profit = revenue minus cost of goods sold
    round(p.total_revenue
          - coalesce(e.avg_unit_cost, 0)
          * p.total_units_sold, 2)                  as gross_profit,

    -- gross margin % = profit / revenue × 100
    round(
        (p.total_revenue
            - coalesce(e.avg_unit_cost, 0) * p.total_units_sold)
        / nullif(p.total_revenue, 0) * 100
    , 1)                                            as gross_margin_pct,

    p.total_units_sold,
    coalesce(e.total_orders, 0)                     as total_orders,
    current_timestamp()

from       pos_agg   p
left join  erp_unit  e
        on  p.store_id = e.store_id
       and  p.category = e.category;

-- ── verify — all margins should now be positive ───────────────
select
    category,
    round(sum(total_revenue), 2)    as total_revenue,
    round(sum(total_cost), 2)       as total_cost,
    round(sum(gross_profit), 2)     as gross_profit,
    round(avg(gross_margin_pct), 1) as avg_margin_pct
from   global_data_mart.marts.fct_gross_margin
group by category
order by avg_margin_pct desc;


select * from global_data_mart.marts.fct_gross_margin;


select distinct(sensor_name) from global_data_mart.staging.stg_iot_sensor_readings;


create or replace table global_data_mart.marts.fct_store_iot_daily (
    event_date         date,                    -- fk → dim_date.date_key
    store_id           varchar(10),             -- fk → dim_store.store_id
    store_name         varchar(100),
    avg_temp_c         float,                   -- cold_storage sensor
    max_temp_c         float,                   -- peak temperature that day
    avg_weight_kg      float,                   -- shelf_weight sensor
    avg_footfall       float,                   -- entrance_gate sensor
    total_footfall     integer,                 -- total visitors that day
    avg_occupancy_pct  float,                   -- entrance_gate sensor
    avg_humidity_pct   float,                   -- cold_storage sensor
    avg_power_kw       float,                   -- energy_meter sensor
    avg_queue_length   float,                   -- pos_terminal sensor
    avg_voltage        float,                   -- energy_meter sensor
    device_count       integer,                 -- count distinct device_id
    low_battery_cnt    integer,                 -- devices with battery_pct < 20
    event_count        integer,                 -- total iot events that day
    updated_at         timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30
comment = 'fact: iot sensors pivoted — grain=store+date. all 9 sensors as columns via case when';

insert into global_data_mart.marts.fct_store_iot_daily
select
    date(event_ts)                                                   as event_date,
    store_id,
    store_name,
    -- temp_c: cold_storage sensor
    round(avg(case when sensor_name = 'temp_c'
              then sensor_value end), 2)                            as avg_temp_c,
    round(max(case when sensor_name = 'temp_c'
              then sensor_value end), 2)                            as max_temp_c,
    -- weight_kg: shelf_weight sensor
    round(avg(case when sensor_name = 'weight_kg'
              then sensor_value end), 2)                            as avg_weight_kg,
    -- footfall: entrance_gate sensor
    round(avg(case when sensor_name = 'footfall'
              then sensor_value end), 2)                            as avg_footfall,
    sum(case when sensor_name = 'footfall'
             then sensor_value end)::integer                        as total_footfall,
    -- occupancy_pct: entrance_gate sensor
    round(avg(case when sensor_name = 'occupancy_pct'
              then sensor_value end), 2)                            as avg_occupancy_pct,
    -- humidity_pct: cold_storage sensor
    round(avg(case when sensor_name = 'humidity_pct'
              then sensor_value end), 2)                            as avg_humidity_pct,
    -- power_kw: energy_meter sensor
    round(avg(case when sensor_name = 'power_kw'
              then sensor_value end), 2)                            as avg_power_kw,
    -- queue_length: pos_terminal sensor
    round(avg(case when sensor_name = 'queue_length'
              then sensor_value end), 2)                            as avg_queue_length,
    -- voltage: energy_meter sensor
    round(avg(case when sensor_name = 'voltage'
              then sensor_value end), 2)                            as avg_voltage,
    count(distinct device_id)                                       as device_count,
    count(distinct case when battery_pct < 20
          then device_id end)                                       as low_battery_cnt,
    count(distinct event_id)                                        as event_count,
    current_timestamp()
from   global_data_mart.staging.stg_iot_sensor_readings
group by date(event_ts), store_id, store_name;

-- verify — check breach days
select
    event_date, store_name,
    avg_temp_c, max_temp_c,
    avg_footfall, low_battery_cnt
from   global_data_mart.marts.fct_store_iot_daily
where  avg_temp_c > 25
order by max_temp_c desc;


select
    event_date, store_name,
    avg_temp_c, max_temp_c,
    avg_footfall, low_battery_cnt
from   global_data_mart.marts.fct_store_iot_daily;


create or replace table global_data_mart.marts.fct_sales_vs_iot (
    report_date        date,                    -- fk → dim_date.date_key
    store_id           varchar(10),             -- fk → dim_store.store_id
    store_name         varchar(100),
    category           varchar(50),
    -- pos columns from fct_daily_sales
    total_revenue      float,
    total_txns         integer,
    avg_basket         float,
    unique_customers   integer,
    -- iot columns from fct_store_iot_daily
    avg_temp_c         float,
    max_temp_c         float,
    avg_footfall       float,
    total_footfall     integer,
    avg_shelf_weight   float,                   -- avg_weight_kg renamed for clarity
    avg_power_kw       float,
    avg_queue_length   float,
    avg_occupancy_pct  float,
    device_count       integer,
    low_battery_cnt    integer,
    -- derived operational flags
    is_temp_breach     boolean,                 -- avg_temp_c > 25
    is_low_stock       boolean,                 -- avg_weight_kg < 5
    is_overcrowded     boolean,                 -- avg_occupancy_pct > 80
    operational_status varchar(20),             -- normal / temp_breach / low_stock / overcrowd
    updated_at         timestamp_ntz default current_timestamp()
)
data_retention_time_in_days = 30
comment = 'fact: 3-way master table — pos + iot on store_id+date. grain=date+store+category';

insert into global_data_mart.marts.fct_sales_vs_iot
select
    s.report_date,
    s.store_id,
    s.store_name,
    s.category,
    -- pos metrics
    s.total_revenue,
    s.total_txns,
    s.avg_basket,
    s.unique_customers,
    -- iot metrics (null if no iot data for that store-day)
    i.avg_temp_c,
    i.max_temp_c,
    i.avg_footfall,
    i.total_footfall,
    i.avg_weight_kg    as avg_shelf_weight,
    i.avg_power_kw,
    i.avg_queue_length,
    i.avg_occupancy_pct,
    i.device_count,
    i.low_battery_cnt,
    -- operational flags derived from iot readings
    coalesce(i.avg_temp_c > 25, false)          as is_temp_breach,
    coalesce(i.avg_weight_kg < 5, false)         as is_low_stock,
    coalesce(i.avg_occupancy_pct > 80, false)    as is_overcrowded,
    case
        when i.avg_temp_c      > 25  then 'temp_breach'
        when i.avg_occupancy_pct > 80 then 'overcrowd'
        when i.avg_weight_kg   < 5   then 'low_stock'
        else                              'normal'
    end                                          as operational_status,
    current_timestamp()
from      global_data_mart.marts.fct_daily_sales      s
left join global_data_mart.marts.fct_store_iot_daily  i
       on  s.store_id    = i.store_id
      and  s.report_date = i.event_date;

-- verify — shows impact of operational status on revenue
select
    operational_status,
    count(*)                          as store_day_count,
    round(avg(total_revenue), 2)      as avg_revenue,
    round(avg(avg_footfall), 2)       as avg_footfall,
    round(avg(avg_temp_c), 2)         as avg_temp
from   global_data_mart.marts.fct_sales_vs_iot
group by operational_status
order by avg_revenue desc;


create or replace materialized view global_data_mart.marts.mv_store_revenue as
select
    store_id,
    store_name,
    store_city,
    store_region,
    report_date,                               -- keep: filter applied at query time
    round(sum(total_revenue), 2) as revenue,
    sum(total_txns)              as txns,
    round(avg(avg_basket), 2)    as avg_basket,
    sum(unique_customers)        as customers
from  global_data_mart.marts.fct_daily_sales
group by
    store_id, store_name, store_city,
    store_region, report_date;

-- ── query examples for mv_store_revenue ──────────────────────

-- last 30 days relative to your actual data (works for 2023 or any year)
select dateadd('day', -30, max(report_date) ) from global_data_mart.marts.fct_daily_sales;


select store_name, store_city,
    sum(revenue)   as revenue_last_30d,
    sum(txns)      as txns_last_30d,
    sum(customers) as customers_last_30d
from  global_data_mart.marts.mv_store_revenue
where report_date >= (
    select dateadd('day', -30, max(report_date))
    from   global_data_mart.marts.fct_daily_sales
)
group by store_name, store_city
order by revenue_last_30d desc;

-- q4 2023 — fixed date range for your dataset
select store_name, store_city, store_region,
    sum(revenue)   as q4_revenue,
    sum(txns)      as q4_txns
from  global_data_mart.marts.mv_store_revenue
where report_date between '2023-10-01' and '2023-12-31'
group by store_name, store_city, store_region
order by q4_revenue desc;


create or replace materialized view global_data_mart.marts.mv_category_revenue as
select
    category,
    store_region,
    round(sum(total_revenue), 2) as revenue,
    sum(total_units)             as units_sold,
    sum(total_txns)              as transactions,
    count(*)                     as row_count    -- not count(distinct) — mv restriction
from  global_data_mart.marts.fct_daily_sales
group by category, store_region;

-- query
select * from global_data_mart.marts.mv_category_revenue
order by revenue desc;



create or replace materialized view global_data_mart.marts.mv_margin_summary as
select
    store_id,
    store_name,
    store_city,
    round(sum(total_revenue), 2)     as total_revenue,
    round(sum(total_cost), 2)        as total_cost,
    round(sum(gross_profit), 2)      as total_profit,
    round(avg(gross_margin_pct), 1)  as avg_margin_pct,
    sum(total_units_sold)            as total_units
from  global_data_mart.marts.fct_gross_margin
group by store_id, store_name, store_city;

-- query — stores ranked by margin
select store_name, store_city,
    total_revenue, total_cost, total_profit, avg_margin_pct
from  global_data_mart.marts.mv_margin_summary
order by avg_margin_pct desc;


create or replace  view global_data_mart.marts.v_category_revenue as
select
    category,
    store_region,
    round(sum(total_revenue), 2) as revenue,
    sum(total_units)             as units_sold,
    count(distinct store_id)     as stores_selling  -- allowed in regular view
from  global_data_mart.marts.fct_daily_sales
group by category, store_region;

-- query
select * from global_data_mart.marts.v_category_revenue
order by revenue desc;




create or replace view global_data_mart.raw.v_iot_alerts as
select
    e.event_id,
    e.store_id,
    e.store_name,
    e.event_ts,
    e.event_type,
    e.device_id,
    a.value:alert_type::varchar           as alert_type,
    a.value:severity::varchar             as severity,
    a.value:triggered_at::timestamp_ntz   as triggered_at
from  global_data_mart.raw.iot_json_raw e
    , lateral flatten(input => e.raw_payload:alerts) a
where array_size(e.raw_payload:alerts) > 0;

-- query — alert summary by store
select
    store_name, alert_type, severity,
    count(*) as alert_count
from  global_data_mart.raw.v_iot_alerts
group by store_name, alert_type, severity
order by alert_count desc;


select
    category,
    round(sum(total_revenue), 2)      as total_revenue,
    sum(total_units)                  as total_units_sold,
    sum(total_txns)                   as total_transactions,
    round(avg(avg_basket), 2)         as avg_basket_size,
    rank() over (order by sum(total_revenue) desc)  as revenue_rank
from   global_data_mart.marts.fct_daily_sales
group by category
order by revenue_rank;



use database global_data_mart; use warehouse compute_wh;

select
    report_date,
    store_name,
    sum(total_revenue)                                        as daily_revenue,
    round(
        avg(sum(total_revenue)) over (
            partition by store_name
            order by     report_date
            rows between 6 preceding and current row
        )
    , 2)                                                      as revenue_7d_ma,
    round(
        100.0 * (
            sum(total_revenue)
            - lag(sum(total_revenue))
                over (partition by store_name order by report_date)
        )
        / nullif(
            lag(sum(total_revenue))
                over (partition by store_name order by report_date), 0
        )
    , 1)                                                      as day_over_day_pct
from   global_data_mart.marts.fct_daily_sales
group by report_date, store_name
order by store_name, report_date;



select
    store_name, store_city, category,
    total_revenue,  total_cost, gross_profit, gross_margin_pct,
    rank()   over (partition by category
                   order by gross_margin_pct desc)  as rank_in_category,
    ntile(4) over (order by total_revenue desc)     as revenue_quartile,
    round(
        100.0 * total_revenue
        / sum(total_revenue) over ()
    , 2)                                            as pct_of_total_revenue,
    case
        when gross_margin_pct >= 35 then 'high'
        when gross_margin_pct >= 30 then 'medium'
        when gross_margin_pct >= 25 then 'low'
        else                             'loss risk'
    end                                             as margin_band
from   global_data_mart.marts.fct_gross_margin
order by category, rank_in_category;


select
    store_name,
    report_date,
    category,
    total_revenue,
    total_txns,
    avg_temp_c,
    max_temp_c,
    avg_footfall,
    avg_shelf_weight,
    operational_status,
    is_temp_breach,
    is_low_stock,
    -- revenue delta vs that store's daily average
    round(
        total_revenue
        - avg(total_revenue) over (partition by store_id)
    , 2)                                            as revenue_vs_store_avg,
    -- revenue delta % vs store average
    round(
        100.0 * (total_revenue
                 - avg(total_revenue) over (partition by store_id))
        / nullif(avg(total_revenue) over (partition by store_id), 0)
    , 1)                                            as revenue_delta_pct
from   global_data_mart.marts.fct_sales_vs_iot
where  avg_temp_c is not null
order by revenue_vs_store_avg asc;   -- worst performing store-days first

with monthly as (
    select
        d.month_name,
        d.month_number,
        d.quarter_name,
        d.year,
        d.month_year,
        sum(f.total_revenue)  as revenue,
        sum(f.total_txns)     as transactions,
        sum(f.total_units)    as units
    from      global_data_mart.marts.fct_daily_sales f
    join      global_data_mart.marts.dim_date        d
           on f.report_date = d.date_key
    group by
        d.month_name, d.month_number, d.quarter_name,
        d.year, d.month_year
)
select
    month_year,
    month_name,
    quarter_name,
    round(revenue, 2)                                as monthly_revenue,
    round(transactions, 0)                           as monthly_txns,
    round(
        lag(revenue) over (order by year, month_number)
    , 2)                                             as prev_month_revenue,
    round(
        100.0 * (revenue - lag(revenue) over (order by year, month_number))
        / nullif(lag(revenue) over (order by year, month_number), 0)
    , 1)                                             as mom_growth_pct
from monthly
order by year, month_number;


select
    e.supplier_id,
    s.supplier_name,
    s.supplier_city,
    e.category,
    count(distinct e.order_id)                      as total_orders,
    sum(e.quantity_ordered)                         as total_qty_ordered,
    sum(e.quantity_received)                        as total_qty_received,
    round(sum(e.total_cost), 2)                     as total_procurement_cost,
    round(avg(e.lead_time_days), 1)                 as avg_lead_time_days,
    sum(case when e.is_late = false then 1 else 0 end) as on_time_deliveries,
    round(
        100.0 * sum(case when e.is_late = false then 1 else 0 end)
        / nullif(count(distinct e.order_id), 0)
    , 1)                                            as on_time_rate_pct,
    count(case when e.order_status = 'delayed'
               then 1 end)                          as delayed_orders
from      global_data_mart.staging.stg_erp_orders    e
left join global_data_mart.marts.dim_supplier        s
       on e.supplier_id = s.supplier_id
where  e.order_id is not null
group by e.supplier_id, s.supplier_name, s.supplier_city, e.category
order by on_time_rate_pct desc;



select distinct
    store_name,
    store_id,
    device_id,
    event_type,
    battery_pct,
    firmware,
    store_floor,
    case
        when battery_pct < 10  then 'critical'
        when battery_pct < 20  then 'low'
        else                        'ok'
    end as battery_status,
    -- how many days since we last saw this device active
    datediff('day',
        max(date(event_ts)) over (partition by device_id),
        (select max(date(event_ts)) from global_data_mart.staging.stg_iot_sensor_readings)
    ) as days_since_last_seen
from   global_data_mart.staging.stg_iot_sensor_readings
where  battery_pct < 20
order by battery_pct asc, store_name;


-- ──────────────────────────────────────────────────────────────
-- query 8: master row count — all layers
-- run this after executing everything to verify the full pipeline
-- ──────────────────────────────────────────────────────────────


select 'dim  : dim_store'             as layer_table, count(*) as rows1 from global_data_mart.marts.dim_store
union all select 'dim  : dim_product',               count(*) from global_data_mart.marts.dim_product
union all select 'dim  : dim_date',                  count(*) from global_data_mart.marts.dim_date
union all select 'dim  : dim_supplier',              count(*) from global_data_mart.marts.dim_supplier
union all select 'fact : fct_daily_sales',           count(*) from global_data_mart.marts.fct_daily_sales
union all select 'fact : fct_gross_margin',          count(*) from global_data_mart.marts.fct_gross_margin
union all select 'fact : fct_store_iot_daily',       count(*) from global_data_mart.marts.fct_store_iot_daily
union all select 'fact : fct_sales_vs_iot',          count(*) from global_data_mart.marts.fct_sales_vs_iot
union all select 'mv   : mv_store_revenue',          count(*) from global_data_mart.marts.mv_store_revenue
union all select 'mv   : mv_category_revenue',       count(*) from global_data_mart.marts.mv_category_revenue
union all select 'mv   : mv_margin_summary',         count(*) from global_data_mart.marts.mv_margin_summary
order by 1;


select * 
from table(information_schema.task_history(
  scheduled_time_range_start => dateadd('hour', -1, current_timestamp())
));


use global_data_mart;



show tasks in account;
alter task global_data_mart.utilities.task_erp_to_silver suspend;

alter task global_data_mart.utilities.task_iot_to_silver suspend;


alter task global_data_mart.utilities.task_pos_to_silver suspend;
