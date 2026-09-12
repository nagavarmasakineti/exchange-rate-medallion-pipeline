# Exchange Rate Data Pipeline (Medallion Architecture)
An end-to-end, containerized Python data engineering pipeline that ingest daily currency exchange rates from external REST APIs, cleanses and validates records using 'pandas' and row-level Audit Balance Controls (ABC), also loads final data into a PostgreSQL Star Schema.

## Architectural Overview
```text
 +---------------------+
 | External REST API   |
 +----------+----------+
            |
            v
 +---------------------+
 |    Bronze Layer     |  <-- Raw JSON Payloads / Staging Tables
 +----------+----------+
            |
            v
 +---------------------+
 |    Silver Layer     |  <-- Pandas Transformations & Deduplication
 +----------+----------+      └─► Audit Balance Control (ABC) Check
            |
            v
 +---------------------+
 |     Gold Layer      |  <-- Fact Table Upserts & Dimensional Joins
 +---------------------+
```
## Medallion Architecture Layer Breakdown

### 1. Bronze Layer (Raw Ingestion & Immutable Staging)
* *Raw REST Ingestion:* Fetches the raw JSON exchange rate response directly from the external Exchange Rate API using requests.get() with explicit 20-second connection timeouts.
* *Status Verification:* Enforces response.raise_for_status() to instantly flag HTTP network errors or bad responses before writing downstream.
* *Immutable Raw Storage:* Saves the unparsed raw JSON payload string as-is into the PostgreSQL bronze_exchange_rates staging table.
* *Fault Isolation:* Preserves raw historical payloads permanently, allowing downstream data to be re-processed anytime without re-calling the external API.

### 2. Silver Layer (Cleansing, Vectorization & Audit Reconciliation)
* *JSON Parsing & Standardization:* Reads the raw JSON from Bronze, extracts base metadata, and converts Unix timestamps into standard Postgres YYYY-MM-DD date strings.
* *Pandas Vectorized Transformation:* Leverages pandas.DataFrame vectorization to instantly unpack and shape nested conversion rates into clean rows (tar_currency, rate_value).
* *Quality Validation Gate:* Applies boolean masking to evaluate records against data quality rules (ensuring rate values are strictly positive and greater than zero).
* *Exception Handling Routing:* Routes invalid/failed records directly to the silver_data_quality_errors table for auditing rather than halting the entire pipeline.
* *Idempotent Bulk Upsert:* Loads validated clean records into silver_exchange_rates using execute_values with ON CONFLICT (base_currency, target_currency, exchange_date) DO UPDATE logic to eliminate duplicate entries.
* *Audit Balance Control (ABC):* Reconciles pipeline data balance using strict mathematical accounting:
  $$\text{Input Count (Bronze)} = \text{Clean Output (Silver)} + \text{Exception Count}$$
  Halts execution immediately if row count leakage is detected.

### 3. Gold Layer (Dimensional Modeling & Advanced Analytics)
* *SQL Analytical Window Functions:* Computes dynamic rolling analytics (AVG(rate) OVER (PARTITION BY target_currency ORDER BY exchange_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) to calculate a 7-day moving average exchange rate.
* *Star Schema Joins:* Joins processed Silver records with dim_currencies and dim_dates dimension tables to resolve surrogate surrogate keys (currency_key, date_key).
* *Fact Table Population:* Populates the fact_exchange_rates table with production metrics (exchange_rate, rolling_7day_avg).
* *Batch Idempotency:* Uses extras.execute_batch with ON CONFLICT (date_key, currency_key) DO UPDATE to ensure safe, repeatable production execution without data duplication.
* *Transaction Safety & Rollback:* Uses full PostgreSQL transaction management (conn.commit() and conn.rollback()) wrapped in try...finally blocks to safely close cursors and database connections.

## LOGS Captured
```text
2026-09-12 14:57:47,072 - INFO - ===============================================================
2026-09-12 14:57:47,072 - INFO - STARTING EXCHANGE RATE DATA PIPELINE
2026-09-12 14:57:47,072 - INFO - ===============================================================
2026-09-12 14:57:47,078 - INFO - BRONZE Phase Started
2026-09-12 14:57:49,028 - INFO - Pipeline execution started.
2026-09-12 14:57:50,409 - INFO - Raw payload successfully loaded
2026-09-12 14:57:50,410 - INFO - Data Saved Successfully to BRONZE
2026-09-12 14:57:50,528 - INFO - pipeline executed successfully
2026-09-12 14:57:50,529 - INFO - BRONZE Phase Completed Successfully
2026-09-12 14:57:50,529 - INFO - SILVER Phase Started
2026-09-12 14:57:54,835 - INFO - **** process_bronze_to_silver ****
2026-09-12 14:57:54,891 - INFO - Fetching Data from BRONZE RAW table
2026-09-12 14:57:55,034 - INFO - 
                ============================================================
                AUDIT BALANCE CONTROL (ABC) SHEET
                ============================================================
                Input (BRONZE SOURCE COUNT) : 166
                Output (Silver Main Table) : 166
                Output (Silver Exception Table) : 0
                Total Recounciled SUM : 166
                ============================================================
            
2026-09-12 14:57:55,034 - INFO - RECOUNCILATION SUCCESSFULL : All rows accounted for Math Balance Successfully.
2026-09-12 14:57:55,035 - INFO - SILVER Phase Completed Successfully
2026-09-12 14:57:55,035 - INFO - GOLD Phase Started
2026-09-12 14:57:55,045 - INFO - ***** process_silever_to_gold *****
2026-09-12 14:57:55,122 - INFO - Successfully Connected to Database
2026-09-12 14:57:55,122 - INFO - Joining Silver data with Dimensions and calculating rolling metrics...
2026-09-12 14:57:55,214 - INFO - Upserting 166 rows into fact_exchange_rates...
2026-09-12 14:57:55,273 - INFO - ** Gold Star Schema update completed successfully! **
2026-09-12 14:57:55,274 - INFO - GOLD Phase Completed Successfully
2026-09-12 14:57:55,274 - INFO - ===============================================================
2026-09-12 14:57:55,275 - INFO - PIPELINE RUN COMPLETED SUCCESSFULLY WITHOUT ERRORS
2026-09-12 14:57:55,276 - INFO - ===============================================================
```
