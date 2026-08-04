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
-- FIX: stream_pos_new now points DIRECTLY at pos_transactions (Bronze),
-- not at a disconnected one-time snapshot. The old "pos_transactions_raw as
-- select * from pos_transactions" table has been REMOVED — it was a frozen
-- copy that nothing ever wrote to again, so the stream built on it never
-- saw new data.

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

-- FIX: iot_task now inserts only the (data) column, matching the stream's
-- actual usable output — select * on a stream also returns METADATA$ACTION,
-- METADATA$ISUPDATE, METADATA$ROW_ID, which don't match iot_events_processed's
-- 2-column shape and would fail the insert.
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
at(timestamp => dateadd(minute, -1, current_timestamp()));

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
