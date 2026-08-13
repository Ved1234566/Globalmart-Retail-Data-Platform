
use database global_data_mart;
use warehouse compute_wh;

-- ================================================================
-- SECTION A — SILVER TABLES
-- ================================================================

create or replace table global_data_mart.setup_schema.iot_events_processed (
    data          variant,
    processed_ts  timestamp_ntz default current_timestamp()
);

create or replace table global_data_mart.setup_schema.pos_transactions_processed (
    transaction_id    string,
    store_id          string,
    store_name        string,
    store_city        string,
    store_region      string,
    cashier_id        string,
    customer_id       string,
    transaction_date  date,
    transaction_time  string,
    product_sku       string,
    product_name      string,
    category          string,     
    subcategory       string,
    quantity          int,        
    unit_price        float,      
    discount_pct      int,        
    total_amount      float,
    payment_method    string,     
    loyalty_points    int,        
    processed_ts      timestamp_ntz default current_timestamp()
);

create or replace table global_data_mart.setup_schema.erp_orders_processed (
    order_id            string,
    order_date          timestamp_ntz,
    store_id            string,
    store_city          string,
    supplier_id         string,
    supplier_name       string,
    supplier_city       string,
    product_sku         string,
    category             string,
    quantity_ordered    int,
    quantity_received   int,
    unit_cost           float,
    total_cost           float,
    order_status         string,
    expected_delivery    date,
    actual_delivery      date,
    warehouse_id         string,
    lead_time_days       int,
    is_late               boolean,
    processed_ts          timestamp_ntz default current_timestamp()
);

create or replace table global_data_mart.setup_schema.erp_inventory_processed (
    snapshot_date       date,
    store_id            string,
    warehouse_id        string,
    product_sku         string,
    category             string,
    quantity_on_hand    int,
    reorder_level        int,
    max_stock_level      int,
    last_received_date   date,
    processed_ts          timestamp_ntz default current_timestamp()
);


create or replace task global_data_mart.setup_schema.pos_task
    warehouse = compute_wh
    schedule  = '5 minute'
    when      system$stream_has_data('global_data_mart.setup_schema.stream_pos_new')
as
insert into global_data_mart.setup_schema.pos_transactions_processed (
    transaction_id, store_id, store_name, store_city, store_region,
    cashier_id, customer_id, transaction_date, transaction_time,
    product_sku, product_name, category, subcategory, quantity,
    unit_price, discount_pct, total_amount, payment_method, loyalty_points
)
select
    transaction_id, store_id, store_name, store_city, store_region,
    cashier_id, customer_id, transaction_date, transaction_time,
    product_sku, product_name,
    initcap(category)                 as category,          
    subcategory,
    greatest(quantity, 0)              as quantity,         
    greatest(unit_price, 0)            as unit_price,
    coalesce(discount_pct, 0)          as discount_pct,       
    total_amount,
    upper(trim(payment_method))        as payment_method,    
    coalesce(loyalty_points, 0)        as loyalty_points
from global_data_mart.setup_schema.stream_pos_new
where metadata$action = 'INSERT'         
  and transaction_id is not null
  and total_amount > 0;

create or replace task global_data_mart.setup_schema.erp_orders_task
    warehouse = compute_wh
    schedule  = '5 minute'
    when      system$stream_has_data('global_data_mart.setup_schema.stream_erp_orders')
as
merge into global_data_mart.setup_schema.erp_orders_processed as tgt
using (
    select * from global_data_mart.setup_schema.stream_erp_orders
    where metadata$action = 'INSERT'
) as src
on tgt.order_id = src.order_id
when matched then
    update set
        tgt.order_status      = src.order_status,
        tgt.quantity_received = src.quantity_received,
        tgt.actual_delivery   = src.actual_delivery,
        tgt.is_late           = src.is_late,
        tgt.processed_ts      = current_timestamp()
when not matched then
    insert (
        order_id, order_date, store_id, store_city, supplier_id, supplier_name,
        supplier_city, product_sku, category, quantity_ordered, quantity_received,
        unit_cost, total_cost, order_status, expected_delivery, actual_delivery,
        warehouse_id, lead_time_days, is_late, processed_ts
    )
    values (
        src.order_id, src.order_date, src.store_id, src.store_city, src.supplier_id, src.supplier_name,
        src.supplier_city, src.product_sku, src.category, src.quantity_ordered, src.quantity_received,
        src.unit_cost, src.total_cost, src.order_status, src.expected_delivery, src.actual_delivery,
        src.warehouse_id, src.lead_time_days, src.is_late, current_timestamp()
    );

create or replace task global_data_mart.setup_schema.erp_inventory_task
    warehouse = compute_wh
    schedule  = '5 minute'
    when      system$stream_has_data('global_data_mart.setup_schema.stream_erp_inventory')
as
insert into global_data_mart.setup_schema.erp_inventory_processed (
    snapshot_date, store_id, warehouse_id, product_sku, category,
    quantity_on_hand, reorder_level, max_stock_level, last_received_date
)
select
    snapshot_date, store_id, warehouse_id, product_sku, category,
    greatest(quantity_on_hand, 0) as quantity_on_hand,
    reorder_level, max_stock_level, last_received_date
from global_data_mart.setup_schema.stream_erp_inventory
where metadata$action = 'INSERT';

create or replace task global_data_mart.setup_schema.iot_task
    warehouse = compute_wh
    schedule  = '5 minute'
    when      system$stream_has_data('global_data_mart.setup_schema.stream_iot_events')
as
insert into global_data_mart.setup_schema.iot_events_processed (data)
select data
from global_data_mart.setup_schema.stream_iot_events
where metadata$action = 'INSERT';

-- ================================================================
-- SECTION C — VERIFY TASKS AND SILVER TABLES
-- ================================================================

show tasks in schema global_data_mart.setup_schema;

select *
from global_data_mart.setup_schema.pos_transactions_processed
where processed_ts >= dateadd(minute, -5, current_timestamp())
limit 100;

select * from global_data_mart.setup_schema.pos_transactions_processed;
select * from global_data_mart.setup_schema.erp_orders_processed;
select * from global_data_mart.setup_schema.erp_inventory_processed;
select * from global_data_mart.setup_schema.iot_events_processed;

select *
from table(information_schema.copy_history(
    table_name => 'global_data_mart.setup_schema.pos_transactions_processed',
    start_time => dateadd(hours, -24, current_timestamp())
));

-- ================================================================
-- SECTION D — DAILY SALES BUFFER 
-- ================================================================

create or replace transient table global_data_mart.setup_schema.daily_sales_buffer as
select transaction_date as date_key, store_id, product_sku, category,
    count(*) as total_transactions,
    sum(quantity) as total_quantity,
    sum(total_amount) as total_revenue
from global_data_mart.setup_schema.pos_transactions_processed
group by transaction_date, store_id, product_sku, category;

create or replace task global_data_mart.setup_schema.task_refresh_sales_buffer
    warehouse = compute_wh
    after     global_data_mart.setup_schema.pos_task   
as
create or replace transient table global_data_mart.setup_schema.daily_sales_buffer as
select transaction_date as date_key, store_id, product_sku, category,
    count(*) as total_transactions,
    sum(quantity) as total_quantity,
    sum(total_amount) as total_revenue
from global_data_mart.setup_schema.pos_transactions_processed
group by transaction_date, store_id, product_sku, category;

-- ================================================================
-- SECTION E — SCD TYPE 2 (dim_store_scd2)
-- ================================================================

create or replace table global_data_mart.setup_schema.dim_store_scd2 (
    store_key       int autoincrement primary key,  -- surrogate key
    store_id        string,
    store_name      string,
    city_name       string,
    effective_date  date,
    end_date        date,
    is_current      boolean
);

create or replace table global_data_mart.setup_schema.stg_store_updates (
    store_id    string,
    store_name  string,
    store_city  string
);

-- Initial seed load
insert into global_data_mart.setup_schema.dim_store_scd2
    (store_id, store_name, city_name, effective_date, end_date, is_current)
select distinct store_id, store_name, store_city,
    '2026-01-01'::date as effective_date,
    null as end_date,
    true as is_current
from global_data_mart.setup_schema.pos_transactions_processed;

select * from global_data_mart.setup_schema.dim_store_scd2 order by store_id;

-- Expire changed rows
merge into global_data_mart.setup_schema.dim_store_scd2 as tgt
using global_data_mart.setup_schema.stg_store_updates as src
    on tgt.store_id = src.store_id and tgt.is_current = true
when matched and tgt.city_name != src.store_city then
    update set
        tgt.end_date = current_date(),
        tgt.is_current = false;

-- Insert new current rows for the ones just expired
insert into global_data_mart.setup_schema.dim_store_scd2
    (store_id, store_name, city_name, effective_date, end_date, is_current)
select src.store_id, src.store_name, src.store_city,
    current_date() as effective_date,
    null as end_date,
    true as is_current
from global_data_mart.setup_schema.stg_store_updates src
join global_data_mart.setup_schema.dim_store_scd2 tgt
    on src.store_id = tgt.store_id
where tgt.is_current = false
  and tgt.end_date = current_date();

select * from global_data_mart.setup_schema.dim_store_scd2 order by store_id, effective_date;

-- Scheduled task keeps SCD2 expiry running automatically
create or replace task global_data_mart.setup_schema.task_scd2_store_update
    warehouse = compute_wh
    schedule  = '5 minute'
as
merge into global_data_mart.setup_schema.dim_store_scd2 as tgt
using global_data_mart.setup_schema.stg_store_updates as src
    on tgt.store_id = src.store_id and tgt.is_current = true
when matched and tgt.city_name != src.store_city then
    update set
        tgt.end_date = current_date(),
        tgt.is_current = false;

-- ================================================================
-- SECTION F — MATERIALIZED VIEWS / GOLD (now built on SILVER, not bronze)
-- ================================================================

create or replace materialized view global_data_mart.setup_schema.mv_store_daily_revenue as
select store_id, store_name, transaction_date, sum(total_amount) as daily_revenue
from global_data_mart.setup_schema.pos_transactions_processed
group by store_id, store_name, transaction_date;

create or replace view global_data_mart.setup_schema.v_store_revenue_30d as
select store_id, store_name, sum(daily_revenue) as revenue_30d
from global_data_mart.setup_schema.mv_store_daily_revenue
where transaction_date >= dateadd(day, -30, current_date())
group by store_id, store_name;

create or replace materialized view global_data_mart.setup_schema.mv_category_by_region as
select store_region, category, sum(total_amount) as total_revenue, sum(quantity) as total_units
from global_data_mart.setup_schema.pos_transactions_processed
group by store_region, category;

select * from global_data_mart.setup_schema.mv_category_by_region order by total_revenue desc;

-- Gross Margin
create or replace view global_data_mart.setup_schema.v_gross_margin as
select p.store_id, p.category,
    sum(p.total_amount) as total_revenue,
    sum(e.total_cost) as total_cost,
    sum(p.total_amount) - sum(e.total_cost) as gross_margin,
    round((sum(p.total_amount) - sum(e.total_cost)) / nullif(sum(p.total_amount), 0) * 100, 2) as gross_margin_pct
from global_data_mart.setup_schema.pos_transactions_processed p
join global_data_mart.setup_schema.erp_orders_processed e
    on p.store_id = e.store_id
   and p.category = e.category
group by p.store_id, p.category;

select * from global_data_mart.setup_schema.v_gross_margin order by gross_margin_pct;

-- IoT Alerts -> Sales Impact

create or replace view global_data_mart.setup_schema.v_iot_alerts as
select
    r.data:event_id::string as event_id,
    r.data:store_id::string as store_id,
    r.data:timestamp::timestamp as event_timestamp,
    a.value:alert_type::string as alert_type,
    a.value:severity::string as severity,
    a.value:triggered_at::timestamp as triggered_at
from global_data_mart.setup_schema.iot_events_raw r,
    lateral flatten(input => r.data:alerts) a;

create or replace view global_data_mart.setup_schema.v_alert_sales_impact as
select a.store_id, date(a.triggered_at) as alert_date, a.alert_type, a.severity,
    count(*) as alert_count,
    sum(p.total_amount) as same_day_revenue
from global_data_mart.setup_schema.v_iot_alerts a
left join global_data_mart.setup_schema.pos_transactions_processed p
    on a.store_id = p.store_id
   and date(a.triggered_at) = p.transaction_date
group by a.store_id, date(a.triggered_at), a.alert_type, a.severity
order by alert_date;

select * from global_data_mart.setup_schema.v_iot_alerts limit 20;
select * from global_data_mart.setup_schema.v_alert_sales_impact order by alert_date;

-- ================================================================
-- SECTION G — RESUME ALL TASKS (must be last, after all task graph changes)
-- ================================================================

alter task global_data_mart.setup_schema.pos_task resume;
alter task global_data_mart.setup_schema.erp_orders_task resume;
alter task global_data_mart.setup_schema.erp_inventory_task resume;
alter task global_data_mart.setup_schema.iot_task resume;
alter task global_data_mart.setup_schema.task_refresh_sales_buffer resume;
alter task global_data_mart.setup_schema.task_scd2_store_update resume;
