-- Global Data Mart setup: integration, stages, raw tables, pipes, streams, and tasks
use role accountadmin;

create or replace database global_data_mart;
create schema global_data_mart.setup_Schema;

use database global_data_mart;
use warehouse compute_wh;

-- create or replace storage integration s3_int
-- type = external_stage
-- storage_provider = 'S3'
-- enabled = true
-- storage_aws_role_arn = 'arn:aws:iam::363437154840:role/Globla_Mart'
-- storage_allowed_locations = ('s3://bigdata1ved/');

-- desc integration s3_int;

create or replace file format global_data_mart.setup_Schema.json_format
type = json
strip_outer_array = true;

create or replace file format global_data_mart.setup_Schema.parquet_format
type = parquet;

create or replace file format global_data_mart.setup_Schema.csv_format
type = csv
skip_header = 1
field_optionally_enclosed_by = '"';

create or replace stage global_data_mart.setup_Schema.json_stage
url = 's3://bigdata1ved/JSON_FILES/'
storage_integration = s3_int
file_format = global_data_mart.setup_Schema.json_format;

create or replace stage global_data_mart.setup_Schema.csv_stage
url = 's3://bigdata1ved/CSV_FILES/'
storage_integration = s3_int
file_format = global_data_mart.setup_Schema.csv_format;

create or replace stage global_data_mart.setup_Schema.parquet_stage
url = 's3://bigdata1ved/parquet_files/'
storage_integration = s3_int
file_format = global_data_mart.setup_Schema.parquet_format;

list @global_data_mart.setup_Schema.json_stage;
list @global_data_mart.setup_Schema.csv_stage;
list @global_data_mart.setup_Schema.parquet_stage;

-- ============ raw / typed landing tables (BRONZE) ============

create or replace table global_data_mart.setup_Schema.pos_transactions (
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
    loyalty_points int,
    load_ts timestamp_ntz default current_timestamp(),
    source_file string
);

create or replace table global_data_mart.setup_Schema.erp_orders (
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
    is_late boolean,
    load_ts timestamp_ntz default current_timestamp()
);

create or replace table global_data_mart.setup_Schema.erp_inventory (
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

create or replace table global_data_mart.setup_Schema.iot_events_raw (
    data variant,
    load_ts timestamp_ntz default current_timestamp()
);

-- ============ COPY INTO ============

copy into global_data_mart.setup_Schema.pos_transactions (
    transaction_id, store_id, store_name, store_city, store_region,
    cashier_id, customer_id, transaction_date, transaction_time,
    product_sku, product_name, category, subcategory, quantity,
    unit_price, discount_pct, total_amount, payment_method, loyalty_points
)
from @global_data_mart.setup_Schema.csv_stage
file_format = (format_name = global_data_mart.setup_Schema.csv_format)
pattern = '.*pos.*[.]csv'
on_error = 'CONTINUE';

copy into global_data_mart.setup_Schema.erp_orders
from @global_data_mart.setup_Schema.parquet_stage
file_format = (format_name = global_data_mart.setup_Schema.parquet_format)
pattern = '.*erp_orders.*[.]parquet'
match_by_column_name = case_insensitive
on_error = 'CONTINUE';

copy into global_data_mart.setup_Schema.erp_inventory
from @global_data_mart.setup_Schema.parquet_stage
file_format = (format_name = global_data_mart.setup_Schema.parquet_format)
pattern = '.*erp_inventory.*[.]parquet'
match_by_column_name = case_insensitive
on_error = 'CONTINUE';

copy into global_data_mart.setup_Schema.iot_events_raw (data)
from @global_data_mart.setup_Schema.json_stage
file_format = (format_name = global_data_mart.setup_Schema.json_format)
pattern = '.*[.]json'
on_error = 'CONTINUE';

select count(*) from global_data_mart.setup_Schema.pos_transactions; -- 39521
select count(*) from global_data_mart.setup_Schema.erp_orders; -- 45000
select count(*) from global_data_mart.setup_Schema.erp_inventory; -- 180
select count(*) from global_data_mart.setup_Schema.iot_events_raw; -- 12000

-- ============ typed table built from raw json landing table ============

create or replace table global_data_mart.setup_Schema.iot_events as
select
    data:event_id::string as event_id,
    data:event_type::string as event_type,
    data:store_id::string as store_id,
    data:store_name::string as store_name,
    data:timestamp::timestamp as event_timestamp,
    data:device_id::string as device_id,
    data:metadata.firmware::string as firmware,
    data:metadata.battery_pct::int as battery_pct,
    data:metadata.signal_rssi::int as signal_rssi,
    data:metadata.store_floor::int as store_floor
from global_data_mart.setup_Schema.iot_events_raw;

-- ============ pipes ============

create or replace pipe global_data_mart.setup_Schema.pos_pipe
auto_ingest = true
as
copy into global_data_mart.setup_Schema.pos_transactions
from @global_data_mart.setup_Schema.csv_stage
file_format = (format_name = global_data_mart.setup_Schema.csv_format)
pattern = '.*pos.*[.]csv';

create or replace pipe global_data_mart.setup_Schema.erp_orders_pipe
auto_ingest = true
as
copy into global_data_mart.setup_Schema.erp_orders
from @global_data_mart.setup_Schema.parquet_stage
file_format = (format_name = global_data_mart.setup_Schema.parquet_format)
pattern = '.*erp_orders.*[.]parquet'
match_by_column_name = case_insensitive;

create or replace pipe global_data_mart.setup_Schema.erp_inventory_pipe
auto_ingest = true
as
copy into global_data_mart.setup_Schema.erp_inventory
from @global_data_mart.setup_Schema.parquet_stage
file_format = (format_name = global_data_mart.setup_Schema.parquet_format)
pattern = '.*erp_inventory.*[.]parquet'
match_by_column_name = case_insensitive;

create or replace pipe global_data_mart.setup_Schema.iot_pipe
auto_ingest = true
as
copy into global_data_mart.setup_Schema.iot_events_raw (data)
from @global_data_mart.setup_Schema.json_stage
file_format = (format_name = global_data_mart.setup_Schema.json_format)
pattern = '.*[.]json';

-- ============ streams ============

create or replace stream global_data_mart.setup_Schema.stream_pos
on table global_data_mart.setup_Schema.pos_transactions;

create or replace stream global_data_mart.setup_Schema.stream_pos_new
on table global_data_mart.setup_Schema.pos_transactions
append_only = true;

create or replace stream global_data_mart.setup_Schema.stream_erp_orders
on table global_data_mart.setup_Schema.erp_orders;

create or replace stream global_data_mart.setup_Schema.stream_erp_inventory
on table global_data_mart.setup_Schema.erp_inventory;

create or replace stream global_data_mart.setup_Schema.stream_iot_events
on table global_data_mart.setup_Schema.iot_events_raw;


-- ============ external table (partitioned) ============

create or replace external table global_data_mart.setup_Schema.ext_pos_transactions (
    transaction_id string as (value:c1::string),
    store_id string as (value:c2::string),
    store_region string as (value:c5::string),
    transaction_date date as (value:c8::date),
    product_sku string as (value:c9::string),
    total_amount float as (value:c15::float)
)
partition by (transaction_date)
location = @global_data_mart.setup_Schema.csv_stage
auto_refresh = true
file_format = global_data_mart.setup_Schema.csv_format;

select * from global_data_mart.setup_Schema.ext_pos_transactions limit 20;

select *
from table(information_schema.external_table_files(
    table_name => 'global_data_mart.setup_Schema.ext_pos_transactions'
));

create or replace external table global_data_mart.setup_Schema.ext_erp_orders (
    order_id string as (value:order_id::string),
    store_id string as (value:store_id::string),
    order_date date as (value:order_date::date),
    category string as (value:category::string),
    total_cost float as (value:total_cost::float)
)
partition by (order_date)
location = @global_data_mart.setup_Schema.parquet_stage
auto_refresh = true
file_format = global_data_mart.setup_Schema.parquet_format;

create or replace external table global_data_mart.setup_Schema.ext_iot_events (
    event_id string as (value:event_id::string),
    store_id string as (value:store_id::string),
    event_date date as (to_date(value:timestamp::string)),
    alert_type string as (value:alert_type::string)
)
partition by (event_date)
location = @global_data_mart.setup_Schema.json_stage
auto_refresh = true
file_format = global_data_mart.setup_Schema.json_format;

create or replace external table global_data_mart.setup_Schema.ext_erp_inventory (
    store_id string as (value:store_id::string),
    product_sku string as (value:product_sku::string),
    snapshot_date date as (value:snapshot_date::date),
    quantity_on_hand int as (value:quantity_on_hand::int)
)
partition by (snapshot_date)
location = @global_data_mart.setup_Schema.parquet_stage
auto_refresh = true
file_format = global_data_mart.setup_Schema.parquet_format;

