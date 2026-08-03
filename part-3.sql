use database fleet_db;
create or replace schema reporting;
use schema reporting;

-- ============ DIM: VEHICLES ============
create or replace view dim_vehicle as
select bvehicle_id,vin,registration_no,make,model,year,depot,
    fuel_type,odometer_km,status,last_updated from fleet_db.setup_schema.vehicles_processed;

-- ============ FACT: TELEMETRY  ============
create or replace view fct_telemetry as
select vehicle_id,event_ts,date(event_ts) as event_date,speed_kmph,engine_temp_c,
    fuel_pct,rpm,harsh_braking,
    case 
        when engine_temp_c > 105 then 1 
    else 0 end   as high_temp_flag,
    case 
        when fuel_pct < 15 then 1 
        else 0 end as low_fuel_flag
from fleet_db.setup_schema.telemetry_processed;

-- ============ FACT: TELEMETRY DAILY============
create or replace view fct_telemetry_daily as
select
    vehicle_id,
    date(event_ts)  as event_date,
    avg(speed_kmph) as avg_speed_kmph,
    max(speed_kmph) as max_speed_kmph,
    avg(engine_temp_c) as avg_engine_temp_c,
    max(engine_temp_c) as max_engine_temp_c,
    avg(fuel_pct) as avg_fuel_pct,
    min(fuel_pct) as min_fuel_pct,
    sum(case when harsh_braking then 1 else 0 end)   as harsh_braking_events,
    sum(case when engine_temp_c > 105 then 1 else 0 end)   as high_temp_events
from fleet_db.setup_schema.telemetry_processed
group by vehicle_id, date(event_ts);

-- ============ FACT: MAINTENANCE  ============
create or replace view fct_maintenance as
select work_order_id,vehicle_id,service_date,work_type,technician,
    labour_hours,downtime_hours,parts_cost_inr,notes
from fleet_db.setup_schema.maintenance_processed;

-- ============ DATE DIMENSION ============
create or replace view dim_date as
select
    date_key,
    year(date_key) as year,
    month(date_key) as month,
    monthname(date_key) as month_name,
    quarter(date_key) as quarter,
    dayname(date_key) as day_name,
    date_trunc('week', date_key)::date as week_start
from (
    select event_date as date_key from fct_telemetry_daily
    union
    select service_date as date_key from fct_maintenance
);

-- ============ ACTIVE ALERTS VIEW  ============
create or replace view vw_active_alerts as
select
    t.vehicle_id, v.depot, v.make, v.model,
    case when t.max_engine_temp_c > 105 then 'High Temp'
         when t.min_fuel_pct < 15 then 'Low Fuel'
         when t.harsh_braking_events >= 4 then 'Harsh Braking'
    end as alert_type,
    t.event_date
from fct_telemetry_daily t
join dim_vehicle v on t.vehicle_id = v.vehicle_id
where t.max_engine_temp_c > 105 or t.min_fuel_pct < 15 or t.harsh_braking_events >= 4
qualify row_number() over (partition by t.vehicle_id order by t.event_date desc) = 1;

select max(event_ts) from fleet_db.setup_schema.telemetry_processed;
select * from dim_vehicle limit 10;
select * from fct_telemetry_daily limit 10;
select * from fct_maintenance limit 10;
select * from dim_date limit 10;
select * from vw_active_alerts limit 10;

show tasks in schema fleet_db.setup_schema;

insert into fleet_db.setup_schema.telemetry_processed
select
    data:vehicle_id::string     as vehicle_id,
    data:timestamp::timestamp   as event_ts,
    data:speed_kmph::float      as speed_kmph,
    data:engine_temp_c::float   as engine_temp_c,
    data:fuel_pct::float        as fuel_pct,
    data:rpm::int                as rpm,
    data:harsh_braking::boolean as harsh_braking
from fleet_db.setup_schema.telemetry_raw;


select data from fleet_db.setup_schema.telemetry_raw limit 1;

insert into fleet_db.setup_schema.telemetry_processed
select
    data:vehicle_id::string      as vehicle_id,
    data:event_ts::timestamp     as event_ts,
    data:speed_kmph::float       as speed_kmph,
    data:engine_temp_c::float    as engine_temp_c,
    data:fuel_pct::float         as fuel_pct,
    data:rpm::int                 as rpm,
    data:harsh_braking::boolean  as harsh_braking
from fleet_db.setup_schema.telemetry_raw;

select data:vehicle_id, data:speed_kmph, data:rpm, data:harsh_braking
from fleet_db.setup_schema.telemetry_raw limit 1;


insert into fleet_db.setup_schema.telemetry_processed
select
    data:vehicle_id::string      as vehicle_id,
    data:event_ts::timestamp     as event_ts,
    data:speed_kmph::float       as speed_kmph,
    data:engine_temp_c::float    as engine_temp_c,
    data:fuel_pct::float         as fuel_pct,
    data:rpm::int                 as rpm,
    data:harsh_braking::boolean  as harsh_braking
from fleet_db.setup_schema.telemetry_raw;

select * from fct_telemetry_daily limit 10;
select * from vw_active_alerts limit 10;


select
    max(max_engine_temp_c) as hottest_temp,
    min(min_fuel_pct)      as lowest_fuel,
    max(harsh_braking_events) as most_braking_events
from fct_telemetry_daily;

select * from fct_telemetry_daily where max_engine_temp_c > 90 limit 10;


select * from fleet_db.setup_schema.telemetry_processed
where fuel_pct > 100 or engine_temp_c > 150
order by fuel_pct desc;


select vehicle_id, event_date, harsh_braking_events, max_engine_temp_c, min_fuel_pct
from fct_telemetry_daily
where harsh_braking_events >= 4 or max_engine_temp_c > 105 or min_fuel_pct > 100
order by event_date desc;

create or replace view dim_date as
select
    date_key,
    year(date_key)  as year,
    month(date_key)  as month,
    monthname(date_key) as month_name,
    quarter(date_key) as quarter,
    dayname(date_key) as day_name,
    date_trunc('week', date_key) as week_start
from (
    select event_date as date_key from fct_telemetry_daily where event_date is not null
    union
    select service_date as date_key from fct_maintenance where service_date is not null
);
