-- Create test_1 table with transaction revenue and date fields
-- Co-authored with CoCo
USE DATABASE global_data_mart;

-- Merge and insert column transaction_id, store_id, store_name, transaction date and time, year*(transaction_id)
CREATE OR REPLACE TABLE test_1
(
    transaction_id STRING PRIMARY KEY,
    store_id INT,
    store_name CHAR(50),
    time_transaction TIMESTAMP,
    quantity INT,
    unit_price FLOAT,
    total_revenue FLOAT,
    total_revenue_discount FLOAT,
    "year" INT,
    "month" INT
);

SELECT
    transaction_id,
    store_name,
    TO_TIMESTAMP(transaction_date || ' ' || transaction_time),
    quantity,
    unit_price,
    quantity * unit_price as unit_price,
    quantity * unit_price * (1 - discount_pct/100) as discounted_price,
    YEAR(transaction_date) as year_transaction,
    MONTH(transaction_date)
FROM pos_transactions;

select greatest(-1,0) <100;
select count(*) from pos_transactions;

-- selet event_id , store_id , raw_payload , raw_payload:metadata ,raw_payload:metadata:battery
-- CREATE OR REPLACE VIEW fact_sales_daily AS
-- SELECT
--     transaction_date,
--     store_id,
--     store_name,
--     category,
--     SUM(total_amount) AS total_revenue
-- FROM global_data_mart.public.pos_transactions
-- GROUP BY
--     transaction_date,
--     store_id,
--     store_name,
--     category;

-- select * from fact_sales_daily;
-- -------------------------------------------

create or replace table daily_store_category_revenue as
select transaction_date,store_id,store_name,store_city,store_region,category,
    sum(total_amount) as total_revenue,sum(quantity) as units_sold,
    count(*) as total_transactions,
    count(distinct transaction_id) as unique_transactions,
    round(sum(total_amount) / nullif(count(*), 0), 2) as avg_transaction_size
from pos_transactions
group by transaction_date, store_id, store_name, store_city, store_region, category
order by transaction_date, store_id, category;

select * from daily_store_category_revenue;


select transaction_date,store_id,store_name,store_city,store_region,category,
    sum(total_amount) as total_revenue,sum(quantity) as units_sold,
    count(*) as total_transactions,count(distinct transaction_id) as unique_transactions,
    round(sum(total_amount) / nullif(count(*), 0), 2) as avg_transaction_size
from pos_transactions
group by transaction_date, store_id, store_name, store_city, store_region, category
order by transaction_date, store_id, category;
--------------------------------------------------------------------------------------
create or replace view store_daily_revenue_sensors as
select p.store_id, p.store_name,p.transaction_date,
    sum(p.total_amount) as total_revenue,
    avg(s.battery_pct) as avg_battery_pct,
    avg(s.signal_rssi) as avg_signal_rssi,
    case
        when avg(s.battery_pct) is null then 'no sensor data'
        else 'has sensor data'
    end as sensor_value_flag
from pos_transactions p
left join iot_events s
    on p.store_id = s.store_id
   and p.transaction_date = date(s.event_timestamp)
group by p.store_id, p.store_name, p.transaction_date;

select * from store_daily_revenue_sensors order by store_id, transaction_date;

