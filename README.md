# E-Commerce Data Warehouse

A dimensional data warehouse built on the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Raw CSV data is loaded into DuckDB, transformed through dbt staging and mart layers, and orchestrated with Apache Airflow. The final output is a clean star schema ready for BI tools like Tableau or Metabase.

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.11 | Data loading, scripting |
| dbt-core | SQL transformations (staging + marts) |
| DuckDB | Local analytical warehouse |
| Apache Airflow | Pipeline orchestration |
| Docker Compose | Containerized Airflow deployment |
| SQL | All modeling and analytics |

## Data Model

The warehouse follows a star schema with two fact tables and three dimension tables.

```
                  +----------------+
                  |   dim_dates    |
                  |----------------|
                  | date_key (PK)  |
                  | day_of_week    |
                  | month          |
                  | quarter        |
                  | year           |
                  | is_weekend     |
                  +-------+--------+
                          |
    +----------------+    |    +-------------------+
    | dim_customers  |    |    |   dim_products    |
    |----------------|    |    |-------------------|
    | customer_key   |    |    | product_key (PK)  |
    | customer_city  |    |    | category_name_en  |
    | customer_state |    |    | category_name_pt  |
    | first_order_at |    |    | weight_g          |
    | total_orders   |    |    | length/height/    |
    +-------+--------+    |    |   width_cm        |
            |             |    +--------+----------+
            |             |             |
    +-------+-------------+-------------+----------+
    |                fact_orders                    |
    |----------------------------------------------|
    | order_id, order_item_id                      |
    | customer_key (FK) -> dim_customers           |
    | product_key (FK)  -> dim_products            |
    | order_date_key (FK) -> dim_dates             |
    | seller_id, order_status, payment_type        |
    | price, freight_value, item_total             |
    | delivery_delay_days                          |
    +----------------------------------------------+
            |
    +-------+--------------------------------------+
    |            fact_daily_sales                   |
    |----------------------------------------------|
    | sale_date_key (FK) -> dim_dates              |
    | total_orders, total_items                    |
    | total_revenue, total_freight, total_gmv      |
    | avg_item_price, avg_order_value              |
    +----------------------------------------------+
```

## Project Structure

```
ecom-data-warehouse/
  .gitignore
  LICENSE
  README.md
  requirements.txt
  profiles.yml.example
  data/                          # Raw Olist CSVs
    olist_orders_dataset.csv
    olist_customers_dataset.csv
    olist_order_items_dataset.csv
    olist_order_payments_dataset.csv
    olist_products_dataset.csv
    olist_sellers_dataset.csv
    product_category_name_translation.csv
  scripts/
    load_raw_data.py             # Loads CSVs into DuckDB raw schema
  dbt_project/
    dbt_project.yml
    packages.yml
    models/
      staging/                   # Cleans and standardizes raw data
        stg_orders.sql
        stg_customers.sql
        stg_products.sql
        stg_order_items.sql
      marts/                     # Star schema fact + dimension tables
        dim_customers.sql
        dim_products.sql
        dim_dates.sql
        fact_orders.sql
        fact_daily_sales.sql
    seeds/
      product_category_name_translation.csv
    macros/
      generate_date_spine.sql
    tests/
      assert_positive_order_amounts.sql
      assert_no_orphan_order_items.sql
  airflow/
    docker-compose.yml
    Dockerfile
    dags/
      elt_pipeline_dag.py
  queries/                       # Sample analytical queries
    top_products_by_revenue.sql
    customer_cohort_analysis.sql
    daily_sales_trends.sql
```

## Getting Started

### Prerequisites

- Python 3.11+
- pip
- Docker and Docker Compose (only needed for Airflow)

### Quick Setup (Local with DuckDB)

1. Clone the repo and install dependencies:

```bash
git clone https://github.com/soumyabp/ecom-data-warehouse.git
cd ecom-data-warehouse
pip install -r requirements.txt
```

2. Copy the profiles template into the dbt project directory:

```bash
cp profiles.yml.example dbt_project/profiles.yml
```

3. Load the raw CSV data into DuckDB:

```bash
python scripts/load_raw_data.py
```

This creates `dbt_project/warehouse.duckdb` with all raw tables under the `raw` schema.

4. Install dbt packages and run the seed:

```bash
cd dbt_project
dbt deps --profiles-dir .
dbt seed --profiles-dir .
```

5. Run dbt models:

```bash
dbt run --profiles-dir .
```

6. Validate with tests:

```bash
dbt test --profiles-dir .
```

### Running with Airflow

If you want to use the orchestrated pipeline:

```bash
cd airflow
cp .env.example .env
docker compose up --build -d
```

The Airflow UI will be available at `http://localhost:8080` (default login: admin / admin). Enable the `ecommerce_elt_pipeline` DAG to run the full ELT flow on a daily schedule.

## ELT Flow

```
Raw CSVs (data/)
     |
     v
[load_raw_data.py] --> DuckDB raw schema (raw_orders, raw_customers, ...)
     |
     v
[dbt staging models] --> Cleaned views (stg_orders, stg_customers, ...)
     |
     v
[dbt mart models] --> Star schema tables (fact_orders, dim_customers, ...)
     |
     v
[dbt tests] --> Data quality validation
```

1. **Extract/Load**: `load_raw_data.py` reads the Olist CSVs and creates raw tables in DuckDB.
2. **Staging**: dbt staging models clean column types, parse timestamps, deduplicate records, standardize text, and join reference data (like translating product categories to English).
3. **Marts**: Dimension tables (customers, products, dates) and fact tables (order line items, daily sales aggregates) form the star schema.
4. **Testing**: Schema tests check for nulls, uniqueness, and valid foreign key relationships. Custom singular tests check for positive order amounts and orphaned line items.

## dbt Commands

| Command | What it does |
|---------|-------------|
| `dbt deps` | Installs packages (dbt_utils) |
| `dbt seed` | Loads the product category translation CSV |
| `dbt run` | Builds all staging and mart models |
| `dbt test` | Runs schema and custom data tests |
| `dbt docs generate` | Generates project documentation |
| `dbt docs serve` | Opens interactive docs in the browser |

All commands should be run from the `dbt_project/` directory with `--profiles-dir .`

## Sample Queries

The `queries/` folder has ready-to-run SQL files. Here are a few examples of what the warehouse supports:

**Top products by revenue:**
```sql
select
    dp.product_category_name_en as category,
    count(*) as items_sold,
    round(sum(fo.price), 2) as total_revenue
from marts.fact_orders fo
join marts.dim_products dp on fo.product_key = dp.product_key
where fo.order_status != 'canceled'
group by dp.product_category_name_en
order by total_revenue desc
limit 10;
```

**Daily revenue with 7-day moving average:**
```sql
select
    sale_date_key,
    total_revenue,
    round(
        avg(total_revenue) over (
            order by sale_date_key
            rows between 6 preceding and current row
        ), 2
    ) as revenue_7d_avg
from marts.fact_daily_sales
order by sale_date_key;
```

**Customer cohort retention:**
```sql
select
    date_trunc('month', dc.first_order_at) as cohort_month,
    count(distinct dc.customer_key) as cohort_size
from marts.dim_customers dc
where dc.first_order_at is not null
group by cohort_month
order by cohort_month;
```

## Dataset

This project uses the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), which contains roughly 100,000 orders placed between 2016 and 2018 on Brazilian marketplaces. The data is real, anonymized commercial data released by Olist.

## Future Improvements

- Add incremental models for fact_orders to handle daily appends without full refreshes
- Implement dbt snapshots for SCD Type 2 tracking on dim_customers
- Add a Snowflake adapter option for cloud deployment
- Set up CI/CD with GitHub Actions to run dbt test on every pull request
- Build a Tableau or Metabase dashboard on top of the mart tables
