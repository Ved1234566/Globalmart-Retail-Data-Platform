-- =====================================================
-- GOLD MART SCHEMA
-- =====================================================

create schema if not exists gold_mart;

use schema gold_mart;

-- =====================================================
-- DAILY SALES REPORT TABLE
-- =====================================================

create or replace table daily_sales_report (

    report_date date,
    store_id string,
    store_city string,
    store_name string,
    category string,

    total_revenue_generated float,
    region_score float,

    total_units_sold number,
    total_transactions number,

    avg_cart_size float,
    total_unique_customers number,

    date_updated timestamp

);

insert into daily_sales_report

select
    transaction_date as report_date,
    store_id,
    store_city,
    store_name,
    category,

    sum(total_amount) as total_revenue_generated,

    round(avg(total_amount),2) as region_score,

    sum(quantity) as total_units_sold,

    count(distinct transaction_id) as total_transactions,

    round(
        sum(total_amount) /
        nullif(count(distinct transaction_id),0),
        2
    ) as avg_cart_size,

    count(distinct customer_id) as total_unique_customers,

    current_timestamp() as date_updated

from global_data_mart.sales.stg_pos_transactions

group by
    transaction_date,
    store_id,
    store_city,
    store_name,
    category;

create or replace view vw_daily_sales_report as
select * from daily_sales_report;

select * from gold_mart.daily_sales_report
limit 20;


use schema gold_mart;

create or replace table total_gross_margin (

    store_name string,
    category string,
    total_revenue_generated float,
    gross_profit float,
    gross_margin_pct float,
    number_of_units_sold int,
    total_unique_orders int,
    date_updated timestamp
);

insert into total_gross_margin

select

    store_name,
    category,

    sum(total_amount) as total_revenue_generated,

    sum(total_amount) * 0.30 as gross_profit,

    round(
        ((sum(total_amount) * 0.30) / nullif(sum(total_amount),0)) * 100,
        2
    ) as gross_margin_pct,

    sum(quantity) as number_of_units_sold,

    count(distinct transaction_id) as total_unique_orders,

    current_timestamp() as date_updated

from global_data_mart.sales.stg_pos_transactions

group by store_name,category;

--====JOINING CSV AND PARQUET========---
create or replace table gold_mart.sales_profit_report as

select

    p.transaction_date,
    p.store_id,
    p.store_name,

    p.product_sku,
    p.product_name,

    p.category,

    sum(p.quantity) as units_sold,

    sum(p.total_amount) as revenue,

    sum(e.raw_data:total_cost::float) as total_cost,

    sum(p.total_amount)
      - sum(e.raw_data:total_cost::float) as gross_profit,

    round(
        (
            (sum(p.total_amount)
            - sum(e.raw_data:total_cost::float))
            / nullif(sum(p.total_amount),0)
        ) * 100,
        2
    ) as gross_margin_pct

from sales.stg_pos_transactions p

join sales.stg_erp_orders e
    on p.product_sku = e.raw_data:sku::string

group by
    p.transaction_date,
    p.store_id,
    p.store_name,
    p.product_sku,
    p.product_name,
    p.category;

select raw_data
from global_data_mart.sales.erp_orders_raw
limit 5;


