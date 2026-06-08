use role accountadmin;
use database global_data_mart;
use schema sales;
use warehouse compute_wh;

-- ================= STORAGE INTEGRATION =================

create or replace storage integration s3_int
type = external_stage
storage_provider = 'S3'
enabled = true
storage_aws_role_arn = 'arn:aws:iam::103714373149:role/Snow_Ved'
storage_allowed_locations = ('s3://declarative-ved/');

-- ================= FILE FORMATS =================

-- JSON format: strip_outer_array removes the outer [] brackets so each object becomes a separate row
create or replace file format json_format
type = json
strip_outer_array = TRUE;

-- CSV format: skip_header=1 ignores the column header row, field_optionally_enclosed_by handles quoted fields
create or replace file format csv_format
type = csv
skip_header = 1
field_optionally_enclosed_by = '"';

-- Parquet format: self-describing columnar format, no extra options needed as schema is embedded in the file
create or replace file format parquet_format
type = parquet;

-- ================= TABLES =================

create or replace table json_data (raw_data variant);

create or replace table pos_transactions (
    transaction_id string, store_id string, store_name string,
    store_city string, store_region string, cashier_id string,
    customer_id string, transaction_date date, transaction_time string,
    product_sku string, product_name string, category string,
    subcategory string, quantity int, unit_price float,
    discount_pct int, total_amount float, payment_method string,
    loyalty_points int
);

-- Single combined raw table for both parquet files i.e the parquet FILES
create or replace table erp_raw (raw_data variant);

-- ================= STAGES =================
-- stages made for S3 to snowflake

-- JASON FILES total - 5 
create or replace stage json_stage
url='s3://declarative-ved/json-files/'
storage_integration = s3_int
file_format = json_format;

-- CSV FILES TOTAL - 4
create or replace stage csv_stage
url='s3://declarative-ved/csv-files/'
storage_integration = s3_int
file_format = csv_format;

--  PARQUET FILES TOTAL 2
create or replace stage parquet_stage
url='s3://declarative-ved/parquet-files/'
storage_integration = s3_int
file_format = parquet_format;

-- ================= VERIFY FILES =================

list @csv_stage;
list @parquet_stage;
list @json_stage;

-- ================= LOAD DATA =================
-- MANUALL COPY INTO 
-- TEMP STORE DATAIN STAGE -> if once creeted we can REFERENCES them using @csv_stage
-- .* — match any characters (any prefix)
-- pos — filename must contain "pos"
-- .* — any characters between "pos" and the extension
-- [.] — a literal dot (escaped because . alone means "any character" in regex)
-- csv — file must end with "csv
copy into pos_transactions
from @csv_stage
file_format = (format_name = csv_format)
pattern = '.*pos.*[.]csv' -- handel 
on_error = 'CONTINUE';

-- Both parquet files load into one raw variant table
copy into erp_raw
from @parquet_stage
file_format = (format_name = parquet_format)
on_error = 'CONTINUE';

copy into json_data
from @json_stage
file_format = (format_name = json_format)
on_error = 'CONTINUE';

-- ================= VERIFY COUNTS =================

select count(*) from pos_transactions;  -- ~200,000
select count(*) from erp_raw;           -- 45,160
select count(*) from json_data;         -- 60,000

-- ================= PIPES =================
-- AUTO COPY INTO 
create or replace pipe pos_pipe
auto_ingest = true
as
copy into pos_transactions
from @csv_stage
file_format = (format_name = csv_format)
pattern = '.*pos.*[.]csv';

create or replace pipe erp_raw_pipe
auto_ingest = true
as
copy into erp_raw
from @parquet_stage
file_format = (format_name = parquet_format);

create or replace pipe json_pipe
auto_ingest = true
as
copy into json_data
from @json_stage
file_format = (format_name = json_format);

-- ================= RAW TABLES =================

create or replace table pos_transactions_raw as
select * from pos_transactions;

create or replace table erp_orders_raw as
select * from erp_raw;

create or replace table iot_events_raw as
select * from json_data;

-- ================= STREAMS =================

create or replace stream stream_pos
on table pos_transactions;

create or replace stream stream_pos_new
on table pos_transactions_raw
append_only = true;

create or replace stream stream_erp_orders
on table erp_orders_raw;

-- ================= SILVER LAYER =================

create or replace table stg_pos_transactions as
select * from pos_transactions_raw;

create or replace table stg_erp_orders as
select * from erp_orders_raw;

-- ================= GOLD LAYER =================

create or replace view sales_summary as
select
    raw_data:category::string as category,
    sum(raw_data:total_cost::float) as total_sales
from stg_erp_orders
group by category;

-- ================= JSON VIEWS =================

create or replace view v_iot_sensor_readings as
select value from iot_events_raw,
lateral flatten(input => raw_data);

create or replace view v_iot_alerts as
select value from iot_events_raw,
lateral flatten(input => raw_data);

-- ================= TASKS =================

create or replace task task_erp
warehouse = compute_wh
schedule = '5 MINUTE'
as
insert into stg_erp_orders
select * from stream_erp_orders;

create or replace task task_pos
warehouse = compute_wh
schedule = '5 MINUTE'
as
insert into stg_pos_transactions
select * from stream_pos_new;

alter task task_erp resume;
alter task task_pos resume;

-- ================= MATERIALIZED VIEW =================

create or replace materialized view mv_sales_summary as
select
    raw_data:category::string as category,
    sum(raw_data:total_cost::float) as total_sales
from stg_erp_orders
group by category;

-- ================= CLONE & TRANSIENT =================

create or replace table erp_orders_clone
clone erp_orders_raw;

create or replace transient table temp_orders as
select * from erp_orders_raw;

create or replace transient table daily_sales_buffer as
select * from erp_orders_raw;

-- ================= TIME TRAVEL =================

select * from erp_orders_raw at(offset => -60);
select * from pos_transactions_raw at(offset => -60);



select * from sales_summary;
select * from mv_sales_summary;

select table_name, is_transient, failsafe_bytes
from information_schema.table_storage_metrics
where table_name = 'DAILY_SALES_BUFFER';