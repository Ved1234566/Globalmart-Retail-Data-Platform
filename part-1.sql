use role accountadmin;

create or replace database global_data_mart;
use database global_data_mart;
use warehouse compute_wh;

create or replace storage integration s3_int
type = external_stage
storage_provider = 'S3'
enabled = true
storage_aws_role_arn = 'arn:aws:iam::363437154840:role/Globla_Mart'
storage_allowed_locations = ('s3://bigdata1ved/');

-- desc integration s3_int;

create or replace file format json_format
type = json
strip_outer_array = true;

create or replace file format parquet_format
type = parquet;

create or replace file format csv_format
type = csv
skip_header = 1
field_optionally_enclosed_by = '"';

create or replace stage json_stage
url = 's3://bigdata1ved/'
storage_integration = s3_int
file_format = json_format;

create or replace stage csv_stage
url = 's3://bigdata1ved/'
storage_integration = s3_int
file_format = csv_format;

create or replace stage parquet_stage
url = 's3://bigdata1ved/'
storage_integration = s3_int
file_format = parquet_format;

list @json_stage;
list @csv_stage;
list @parquet_stage;

-- ============ raw / typed landing tables ============

create or replace table pos_transactions (
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

create or replace table erp_orders (
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

create or replace table erp_inventory (
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


create or replace table iot_events_raw (
    data variant,
    load_ts timestamp_ntz default current_timestamp()
);

-- ============ initial bulk loads ============

copy into pos_transactions
from @csv_stage
file_format = (format_name = csv_format)
pattern = '.*pos.*[.]csv'
on_error = 'CONTINUE';

copy into erp_orders
from @parquet_stage
file_format = (format_name = parquet_format)
pattern = '.*erp_orders.*[.]parquet'
match_by_column_name = case_insensitive
on_error = 'CONTINUE';

copy into erp_inventory
from @parquet_stage
file_format = (format_name = parquet_format)
pattern = '.*erp_inventory.*[.]parquet'
match_by_column_name = case_insensitive
on_error = 'CONTINUE';

copy into iot_events_raw (data)
from @json_stage
file_format = (format_name = json_format)
pattern = '.*[.]json'
on_error = 'CONTINUE';

select count(*) from pos_transactions;
select count(*) from erp_orders;
select count(*) from erp_inventory;
select count(*) from iot_events_raw;

update pos_transactions
set discount_pct = 10
where transaction_id = '1';

select * from pos_transactions at(offset => -60);

-- ============ typed table built from raw json landing table ============

create or replace table iot_events as
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
from iot_events_raw;

-- ============ pipes (auto ingest, mirror the copy statements above) ============

create or replace pipe pos_pipe
auto_ingest = true
as
copy into pos_transactions
from @csv_stage
file_format = (format_name = csv_format)
pattern = '.*pos.*[.]csv';

create or replace pipe erp_orders_pipe
auto_ingest = true
as
copy into erp_orders
from @parquet_stage
file_format = (format_name = parquet_format)
pattern = '.*erp_orders.*[.]parquet'
match_by_column_name = case_insensitive;

create or replace pipe erp_inventory_pipe
auto_ingest = true
as
copy into erp_inventory
from @parquet_stage
file_format = (format_name = parquet_format)
pattern = '.*erp_inventory.*[.]parquet'
match_by_column_name = case_insensitive;

create or replace pipe iot_pipe
auto_ingest = true
as
copy into iot_events_raw (data)
from @json_stage
file_format = (format_name = json_format)
pattern = '.*[.]json';

-- ============ cdc source table for pos (streams need a standard table, not just the copy target) ============

create or replace table pos_transactions_raw as
select * from pos_transactions;

-- ============ streams ============

create or replace stream stream_pos
on table pos_transactions_raw;

create or replace stream stream_pos_new
on table pos_transactions_raw
append_only = true;

create or replace stream stream_erp_orders
on table erp_orders;

create or replace stream stream_erp_inventory
on table erp_inventory;

create or replace stream stream_iot_events
on table iot_events_raw;

-- ============ processed tables ============

create or replace table pos_transactions_processed (
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

create or replace table erp_orders_processed (
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

create or replace table erp_inventory_processed (
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

create or replace table iot_events_processed (
    data variant,
    processed_ts timestamp_ntz default current_timestamp()
);

-- ============ tasks ============

create or replace task pos_task
warehouse = compute_wh
schedule = '5 minute'
as
insert into pos_transactions_processed (
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
from stream_pos_new;

create or replace task erp_orders_task
warehouse = compute_wh
schedule = '5 minute'
as
insert into erp_orders_processed (
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
from stream_erp_orders;

create or replace task erp_inventory_task
warehouse = compute_wh
schedule = '5 minute'
as
insert into erp_inventory_processed (
    snapshot_date, store_id, warehouse_id, product_sku, category,
    quantity_on_hand, reorder_level, max_stock_level, last_received_date
)
select
    snapshot_date, store_id, warehouse_id, product_sku, category,
    quantity_on_hand, reorder_level, max_stock_level, last_received_date
from stream_erp_inventory;

create or replace task iot_task
warehouse = compute_wh
schedule = '5 minute'
as
insert into iot_events_processed (data)
select data
from stream_iot_events;

alter task pos_task suspend;
alter task erp_orders_task suspend;
alter task erp_inventory_task suspend;
alter task iot_task suspend;

alter task pos_task resume;
alter task erp_orders_task resume;
alter task erp_inventory_task resume;
alter task iot_task resume;

-- ============ checks ============

select *
from pos_transactions
at(timestamp => dateadd(minute, -1, current_timestamp()));

describe table erp_orders;
describe table erp_inventory;
describe table iot_events_raw;

--======= CHECKING THE TABLES FOR THTA DATA========--

select * from pos_transactions_raw;
select * from erp_orders;
select * from erp_inventory;
select * from iot_events;


-- stores with high sales 
select p.store_id , sum(p.total_amount) as revenue, avg(e.battery_pct) as avg_sensor_battery from pos_transactions p
join iot_events e on p.store_id = e.store_id
group by p.store_id
order by revenue desc;


-- 1> on a daily basic store id , name , city region and category total revenue , unit sold, total transaction , avg size of transaction and unique transaction
-- 2> every 30 days based on each date , every store , total revenue (running total / cumulative )
-- 3> create a view store ID and name , transaction date and toal_revenue  , avg sensor value i-name avg value  ii weight sensor avg iii- sensor value - transaction and sensor file 
-- left join table and check value , group by , aggregate and case statement 

-- study this 
--- bar , histo , line , pipe graphs 