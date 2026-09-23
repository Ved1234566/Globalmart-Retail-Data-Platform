# Global Data Mart --- AWS S3 & Snowflake Data Engineering Pipeline

## 📌 Project Overview

This project implements an end-to-end **cloud data engineering
pipeline** using **AWS S3 and Snowflake** to ingest, process, transform,
and analyze data from multiple business domains.

The pipeline handles:

-   Point-of-Sale (POS) transactions
-   ERP orders
-   ERP inventory
-   IoT sensor events

The solution follows a layered data architecture:

**AWS S3 → Snowflake Bronze → Silver → Gold / Data Mart → Analytics**

Snowflake features such as **external stages, file formats, `COPY INTO`,
Snowpipe, Streams, Tasks, external tables, SCD Type 2, materialized
views, regular views, and analytical SQL** are used throughout the
project.

------------------------------------------------------------------------

## 🏗️ Architecture

``` text
                         AWS S3
                           │
          ┌────────────────┼────────────────┐
          │                │                │
        CSV              Parquet           JSON
          │                │                │
          ▼                ▼                ▼
   ┌─────────────────────────────────────────────┐
   │              Snowflake                      │
   │                                             │
   │  External Stages + File Formats             │
   │                                             │
   │  ┌───────────────────────────────────────┐  │
   │  │ BRONZE / RAW                          │  │
   │  │ POS Transactions                      │  │
   │  │ ERP Orders                            │  │
   │  │ ERP Inventory                         │  │
   │  │ IoT Raw Events                        │  │
   │  └───────────────────────────────────────┘  │
   │                     │                       │
   │              Snowpipe / Streams             │
   │                     │                       │
   │  ┌───────────────────────────────────────┐  │
   │  │ SILVER / PROCESSED                    │  │
   │  │ Cleansing + Standardization           │  │
   │  │ Incremental Processing                │  │
   │  │ SCD Type 2                            │  │
   │  └───────────────────────────────────────┘  │
   │                     │                       │
   │                   Tasks                     │
   │                     │                       │
   │  ┌───────────────────────────────────────┐  │
   │  │ GOLD / DATA MART                     │  │
   │  │ Dimensions                            │  │
   │  │ Fact Tables                           │  │
   │  │ Materialized Views                    │  │
   │  │ Analytical Views                      │  │
   │  └───────────────────────────────────────┘  │
   └─────────────────────────────────────────────┘
                           │
                           ▼
                  Business Analytics
```

------------------------------------------------------------------------

## ☁️ AWS S3 Integration

AWS S3 is used as the cloud storage layer for incoming datasets.

The project uses separate S3 locations for:

``` text
s3://bigdata1ved/
│
├── JSON_FILES/
├── CSV_FILES/
└── parquet_files/
```

Snowflake uses a storage integration to securely connect to S3.

The SQL project defines Snowflake stages for:

-   JSON
-   CSV
-   Parquet

The stages are then used for data loading and external-table access.

------------------------------------------------------------------------

## 🧰 Technologies Used

  -----------------------------------------------------------------------
  Technology                          Purpose
  ----------------------------------- -----------------------------------
  **AWS S3**                          Cloud object storage

  **Snowflake**                       Cloud data warehouse

  **Snowpipe**                        Automated file ingestion

  **Snowflake Streams**               Change data capture

  **Snowflake Tasks**                 Scheduled/incremental processing

  **SQL**                             Data transformation and analytics

  **JSON**                            IoT event data

  **CSV**                             POS transaction data

  **Parquet**                         ERP datasets

  **Snowflake External Tables**       Query data directly from staged
                                      files

  **Materialized Views**              Precomputed analytical datasets

  **SCD Type 2**                      Historical dimension tracking
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 📂 Data Sources

## 1. POS Transactions

POS data contains information such as:

-   Transaction ID
-   Store
-   Customer
-   Product
-   Category
-   Quantity
-   Unit price
-   Discount
-   Total amount
-   Payment method
-   Loyalty points

The POS source is loaded from CSV files.

------------------------------------------------------------------------

## 2. ERP Orders

ERP order data contains:

-   Order ID
-   Store
-   Supplier
-   Product
-   Category
-   Quantity ordered
-   Quantity received
-   Unit cost
-   Total cost
-   Order status
-   Expected delivery
-   Actual delivery
-   Warehouse
-   Lead time
-   Late-delivery flag

ERP order data is stored in Parquet format.

------------------------------------------------------------------------

## 3. ERP Inventory

Inventory data contains:

-   Snapshot date
-   Store
-   Warehouse
-   Product
-   Category
-   Quantity on hand
-   Reorder level
-   Maximum stock level
-   Last received date

This dataset is also loaded from Parquet files.

------------------------------------------------------------------------

## 4. IoT Sensor Data

IoT events are provided as JSON.

The raw payload contains information such as:

-   Event ID
-   Event type
-   Store
-   Timestamp
-   Device
-   Firmware
-   Battery percentage
-   Signal strength
-   Store floor
-   Alerts

The raw JSON is stored using Snowflake's `VARIANT` data type and
subsequently transformed into typed columns.

------------------------------------------------------------------------

# 🥉 Bronze Layer

The Bronze layer acts as the raw/landing layer.

The project creates raw tables for:

``` text
pos_transactions
erp_orders
erp_inventory
iot_events_raw
```

The POS table stores transaction-level records, ERP tables store
operational data, and IoT JSON is initially stored as semi-structured
`VARIANT` data.

The project also creates a typed IoT table by extracting fields from the
raw JSON payload.

------------------------------------------------------------------------

# 📥 Data Loading

## File Formats

Three Snowflake file formats are defined:

``` sql
JSON
CSV
PARQUET
```

The CSV configuration includes:

-   Header skipping
-   Optional field enclosure

The JSON format is configured with outer-array stripping.

Parquet uses Snowflake's native Parquet file format.

------------------------------------------------------------------------

## COPY INTO

Initial/batch ingestion is performed using `COPY INTO`.

Examples include:

``` text
S3 CSV → POS table
S3 Parquet → ERP Orders
S3 Parquet → ERP Inventory
S3 JSON → IoT raw table
```

File patterns are used to select the appropriate files.

------------------------------------------------------------------------

# 🚀 Snowpipe --- Automated Ingestion

The project implements Snowpipe with:

``` sql
AUTO_INGEST = TRUE
```

Separate pipes are created for:

-   POS transactions
-   ERP orders
-   ERP inventory
-   IoT events

This enables automated ingestion of new files arriving in the
corresponding S3 locations.

------------------------------------------------------------------------

# 🔄 Streams --- Change Data Capture

Snowflake Streams are created on the Bronze tables.

Streams include:

``` text
stream_pos
stream_pos_new
stream_erp_orders
stream_erp_inventory
stream_iot_events
```

Streams allow downstream processing to consume newly inserted or changed
records without repeatedly processing the entire source table.

An append-only stream is also used for POS processing.

------------------------------------------------------------------------

# 🥈 Silver Layer

The Silver layer contains cleaned and processed datasets.

The project creates processed tables for:

``` text
pos_transactions_processed
erp_orders_processed
erp_inventory_processed
iot_events_processed
```

### POS transformations

The POS task performs transformations including:

-   Category standardization using `INITCAP`
-   Non-negative quantity handling
-   Non-negative unit-price handling
-   Null discount replacement
-   Payment-method standardization using `UPPER(TRIM())`
-   Null loyalty-point replacement
-   Filtering invalid transaction IDs
-   Filtering transactions with non-positive totals

Example:

``` sql
initcap(category)
greatest(quantity, 0)
greatest(unit_price, 0)
coalesce(discount_pct, 0)
upper(trim(payment_method))
coalesce(loyalty_points, 0)
```

------------------------------------------------------------------------

## ERP Orders Processing

ERP order processing uses a `MERGE`.

The process:

1.  Reads new records from the stream.
2.  Matches records using `order_id`.
3.  Updates selected attributes when an order already exists.
4.  Inserts new orders when no matching record exists.

This provides incremental processing rather than rebuilding the entire
table.

------------------------------------------------------------------------

## ERP Inventory Processing

Inventory records are processed from the inventory stream.

The quantity-on-hand value is protected from negative values:

``` sql
greatest(quantity_on_hand, 0)
```

------------------------------------------------------------------------

## IoT Processing

IoT events are consumed from the IoT stream and inserted into the
processed IoT table.

The raw JSON data remains available in semi-structured form while
downstream views extract operational alert information.

------------------------------------------------------------------------

# ⏱️ Snowflake Tasks

The project automates processing using Snowflake Tasks.

Tasks include:

``` text
pos_task
erp_orders_task
erp_inventory_task
iot_task
task_refresh_sales_buffer
task_scd2_store_update
```

The main ingestion-processing tasks are scheduled every **5 minutes**
and use stream-data checks where applicable.

Example logic:

``` sql
WHEN SYSTEM$STREAM_HAS_DATA(...)
```

This allows processing to occur when new stream data is available.

------------------------------------------------------------------------

# 📊 Daily Sales Buffer

A transient daily-sales buffer is created to aggregate POS transactions.

The aggregation is performed by:

``` text
Transaction Date
Store
Product SKU
Category
```

Metrics include:

-   Total transactions
-   Total quantity
-   Total revenue

The buffer is refreshed through a dependent Snowflake Task.

------------------------------------------------------------------------

# 🕒 SCD Type 2

The project implements **Slowly Changing Dimension Type 2** for store
information.

The table:

``` text
dim_store_scd2
```

contains:

-   Surrogate store key
-   Store ID
-   Store name
-   City
-   Effective date
-   End date
-   Current-record flag

Structure:

``` text
store_id
store_name
city_name
effective_date
end_date
is_current
```

### SCD Type 2 process

When a store's city changes:

``` text
Current record
     │
     ▼
Expire old record
is_current = FALSE
end_date = current_date
     │
     ▼
Insert new record
is_current = TRUE
effective_date = current_date
```

This preserves historical versions of store attributes.

------------------------------------------------------------------------

# 🥇 Gold Layer / Data Mart

The Gold layer contains business-ready analytical tables.

The project builds dimensional and fact tables in the `MARTS` schema.

## Dimension Tables

### `dim_store`

Stores master store information:

-   Store ID
-   Store name
-   City
-   Region
-   Country
-   Update timestamp

### `dim_product`

Contains product hierarchy information:

-   Product SKU
-   Product name
-   Category
-   Subcategory

### `dim_date`

A calendar/date dimension containing:

-   Date
-   Day of week
-   Day name
-   Day of month
-   Day of year
-   Week
-   Month
-   Quarter
-   Year
-   Weekend flag
-   Month-year
-   Quarter-year

### `dim_supplier`

Contains:

-   Supplier ID
-   Supplier name
-   Supplier city

------------------------------------------------------------------------

# 📈 Fact Tables

## `fct_daily_sales`

The daily sales fact table has a grain of:

``` text
Date + Store + Category
```

Metrics include:

-   Total revenue
-   Total units
-   Total transactions
-   Average basket
-   Unique customers

Example calculations:

``` sql
SUM(line_total)
SUM(quantity)
COUNT(DISTINCT transaction_id)
AVG(line_total)
COUNT(DISTINCT customer_id)
```

------------------------------------------------------------------------

## `fct_gross_margin`

The gross-margin fact table combines POS revenue with ERP cost
information.

It calculates:

``` text
Revenue
Cost
Gross Profit
Gross Margin %
Units Sold
Orders
```

Gross profit:

``` text
Gross Profit = Revenue - Cost
```

Gross margin:

``` text
Gross Margin % =
(Revenue - Cost) / Revenue × 100
```

The implementation first aggregates POS data and ERP unit-cost
information by store and category before joining them to avoid join
fan-out.

------------------------------------------------------------------------

# 🌡️ IoT Store Analytics

## `fct_store_iot_daily`

IoT sensor readings are transformed into daily store-level metrics.

The table contains metrics such as:

-   Average temperature
-   Maximum temperature
-   Average shelf weight
-   Average footfall
-   Total footfall
-   Average occupancy
-   Average humidity
-   Average power
-   Average queue length
-   Average voltage
-   Device count
-   Low-battery device count
-   Event count

The sensor values are pivoted from rows into analytical columns using
conditional aggregation.

Example:

``` sql
AVG(
    CASE
        WHEN sensor_name = 'temp_c'
        THEN sensor_value
    END
)
```

------------------------------------------------------------------------

# 🔗 Sales vs IoT Analytics

The project combines sales data and IoT data into:

``` text
fct_sales_vs_iot
```

The table joins:

``` text
Sales + Store + Date
        │
        ▼
IoT Store Metrics
```

It creates operational flags:

### Temperature breach

``` text
avg_temp_c > 25
```

### Low stock

``` text
avg_weight_kg < 5
```

### Overcrowding

``` text
avg_occupancy_pct > 80
```

An operational status is then derived:

``` text
normal
temp_breach
low_stock
overcrowd
```

This makes it possible to analyze operational conditions alongside sales
performance.

------------------------------------------------------------------------

# 👀 Views & Materialized Views

The project creates several analytical views.

## Store Revenue

``` text
mv_store_revenue
```

Provides store-level revenue and transaction metrics.

## Category Revenue

``` text
mv_category_revenue
v_category_revenue
```

Provides revenue and unit metrics by category and region.

## Margin Summary

``` text
mv_margin_summary
```

Provides:

-   Revenue
-   Cost
-   Profit
-   Margin %
-   Units

## IoT Alerts

``` text
v_iot_alerts
```

Extracts alert information from nested JSON using:

``` sql
LATERAL FLATTEN
```

## Alert Sales Impact

``` text
v_alert_sales_impact
```

Combines IoT alerts with same-day POS revenue to support operational
impact analysis.

------------------------------------------------------------------------

# 📊 Analytical SQL

The project also demonstrates advanced analytical SQL.

## Window Functions

Examples include:

``` text
RANK()
LAG()
NTILE()
AVG() OVER()
SUM() OVER()
```

These are used for:

-   Revenue ranking
-   Day-over-day revenue change
-   Revenue quartiles
-   Moving averages
-   Revenue contribution
-   Store-level comparisons

------------------------------------------------------------------------

## 7-Day Moving Average

The project calculates a 7-day revenue moving average:

``` sql
AVG(SUM(total_revenue)) OVER (
    PARTITION BY store_name
    ORDER BY report_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
```

------------------------------------------------------------------------

## Day-over-Day Revenue Change

Revenue changes are calculated using `LAG()`:

``` text
Current Revenue
      vs
Previous Day Revenue
```

This produces a percentage change for each store and reporting date.

------------------------------------------------------------------------

## Revenue Contribution

The project calculates each record's percentage contribution to total
revenue:

``` text
Revenue / Total Revenue × 100
```

------------------------------------------------------------------------

## Revenue Quartiles

`NTILE(4)` is used to divide revenue records into four groups.

This enables quartile-based revenue analysis.

------------------------------------------------------------------------

## Margin Bands

Gross margin is categorized into business bands:

``` text
High
Medium
Low
Loss Risk
```

The thresholds are implemented directly in the SQL logic.

------------------------------------------------------------------------

# 🔍 Data Validation & Verification

The SQL includes multiple verification queries.

Examples include:

-   Row counts
-   Distinct store counts
-   Distinct categories
-   Date ranges
-   Revenue totals
-   Margin calculations
-   IoT temperature breaches
-   Low-battery devices
-   Operational status
-   Materialized-view outputs

Example:

``` sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT store_id) AS stores,
    COUNT(DISTINCT category) AS categories,
    MIN(report_date) AS first_date,
    MAX(report_date) AS last_date
FROM fct_daily_sales;
```

------------------------------------------------------------------------

# 🌐 External Tables

The project also demonstrates Snowflake External Tables for querying
data stored in S3 without loading the complete dataset into standard
Snowflake tables.

External tables are created for:

``` text
POS transactions
ERP orders
ERP inventory
IoT events
```

They use:

``` text
S3 Stage
+
File Format
+
External Table
```

with automatic refresh enabled.

------------------------------------------------------------------------

# 🔄 End-to-End Data Flow

``` text
1. Source files arrive in AWS S3
                ↓
2. Snowflake Storage Integration
                ↓
3. External Stages
                ↓
4. File Formats
                ↓
5. Snowpipe / COPY INTO
                ↓
6. Bronze Tables
                ↓
7. Snowflake Streams
                ↓
8. Snowflake Tasks
                ↓
9. Silver Processed Tables
                ↓
10. SCD Type 2 / Aggregations
                ↓
11. Gold Dimensions & Fact Tables
                ↓
12. Views / Materialized Views
                ↓
13. Analytical SQL
                ↓
14. Business Insights
```

------------------------------------------------------------------------

# 🗂️ Suggested GitHub Repository Structure

``` text
global-data-mart/
│
├── README.md
│
├── sql/
│   ├── 01_setup_and_bronze.sql
│   ├── 02_silver_processing.sql
│   └── 03_gold_data_mart.sql
│
├── architecture/
│   └── architecture.png
│
├── data/
│   └── sample_data_description.md
│
└── screenshots/
    ├── snowflake_database.png
    ├── snowpipe.png
    ├── streams.png
    ├── tasks.png
    └── data_mart.png
```

------------------------------------------------------------------------

# 🎯 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

-   AWS S3
-   Snowflake
-   Cloud data warehousing
-   Data ingestion
-   Batch processing
-   Incremental processing
-   Snowpipe
-   Streams
-   Tasks
-   Change Data Capture
-   Bronze/Silver/Gold architecture
-   Dimensional modeling
-   Fact and dimension tables
-   SCD Type 2
-   Data cleansing
-   Data aggregation
-   Semi-structured JSON
-   `VARIANT`
-   `LATERAL FLATTEN`
-   External tables
-   Materialized views
-   Regular views
-   Window functions
-   Data validation
-   Operational analytics
-   IoT analytics

------------------------------------------------------------------------

# 💼 Resume Project Description

**AWS S3 & Snowflake Global Data Mart**

> Developed an end-to-end cloud data engineering pipeline using AWS S3
> and Snowflake to ingest POS, ERP, inventory, and IoT datasets across
> CSV, Parquet, and JSON formats. Implemented Snowpipe, Streams, Tasks,
> SCD Type 2, Bronze/Silver/Gold architecture, dimensional modeling,
> fact tables, materialized views, and analytical SQL to create a
> scalable business data mart for sales, margin, inventory, and IoT
> analytics.

------------------------------------------------------------------------

# 🚀 Future Enhancements

Potential extensions for the project include:

-   Add orchestration using AWS Lambda or an external workflow
    orchestrator.
-   Add automated data-quality checks.
-   Add monitoring and alerting for failed Snowpipe/Task executions.
-   Add role-based Snowflake access control.
-   Add Power BI/Tableau dashboards on top of the Gold layer.
-   Add automated CI/CD for Snowflake SQL.
-   Add more historical SCD dimensions.
-   Add automated testing for transformations.
-   Add pipeline execution logging and audit tables.

------------------------------------------------------------------------

# 👨‍💻 Author

**Vedang Sharma**

**Data Engineer \| SQL \| Snowflake \| AWS \| ETL \| Data Warehousing**

------------------------------------------------------------------------

## ⭐ Project Highlights

``` text
AWS S3
   ↓
Snowflake
   ↓
Snowpipe
   ↓
Streams
   ↓
Tasks
   ↓
Bronze
   ↓
Silver
   ↓
SCD Type 2
   ↓
Gold Data Mart
   ↓
Fact + Dimension Tables
   ↓
Materialized Views
   ↓
Business Analytics
```

This project demonstrates an end-to-end approach to building a
cloud-based analytical data platform using AWS and Snowflake.
