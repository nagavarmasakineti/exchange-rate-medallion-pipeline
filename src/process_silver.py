from psycopg2.extras import execute_values #Fastbulk insert helper
import json
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from db import get_db_connection

logger = logging.getLogger("run_pipeline")

# Load ENV Values
load_dotenv()

def process_bronze_to_silver():
    logger.info("**** process_bronze_to_silver ****")
    conn = get_db_connection()
    try: 
        with conn.cursor() as cur:
            # Read unparsed data from BRONZE
            logger.info("Fetching Data from BRONZE RAW table")
            cur.execute("SELECT raw_payload FROM bronze_exchange_rates order by id desc LIMIT 1;")
            row = cur.fetchone()


            if not row:
                print("No Records found in BRONZE table to process")
                return
            raw_payload = row[0]
            data = json.loads(raw_payload)

            #Mapping the result of Data to SILVER table
            base = data["base_code"]
            # 1. Grab the clean unix integer timestamp from the JSON
            unix_ts = data.get("time_last_update_unix")
            
            # 2. Safely convert it into a YYYY-MM-DD string format for Postgres
            exchange_date = datetime.fromtimestamp(unix_ts).strftime('%Y-%m-%d')
            rates = data["conversion_rates"]

            #------------------------------------------------------------------------------------
            # PANDAS VECTORIZATION LAYER
            #------------------------------------------------------------------------------------

            # 1. Convert the entire rates dictionary into a dataframe instantly
            df = pd.DataFrame(list(rates.items()), columns=['tar_currency', 'rate_value'])

            #2. Vector add the scalar metadata across all the rows
            df['base_currency'] = base
            df['exchange_date'] = exchange_date

            total_currencies_received = len(df)

            # 3. Boolean Masking to split clean data from validation errors
            invalid_mask = df['rate_value'] < 0
            df_errors = df[invalid_mask].copy()
            df_clean = df[~invalid_mask].copy()

            #Track count using dataframe data length
            rows_sent_to_exception = len(df_errors)
            rows_inserted_updated = len(df_clean)

            # 4. Bult Insert Data (if any exceptions)
            if rows_sent_to_exception > 0:
                #df_errors['error_msg'] = f"Validation failed: Zero or Negative Exchange Rate Detected ({tar_currency} {rate_value})"
                df_errors['error_msg'] = df_errors.apply(
                    lambda r: f"Validation failed: Zero or Negative Exchange Rate Detected ({r['tar_currency']}) ({r['rate_value']})",
                    axis = 1
                )

                error_query = """INSERT INTO silver_data_quality_errors (base_currency, target_currency, invalid_rate, error_reason)
                    VALUES %s"""
                
                # Prepare Data tuple for bulk extraction
                error_records = list(df_errors[['base_currency', 'tar_currency', 'rate_value', 'error_msg']].itertuples(index = False, name = None))
                execute_values(cur, error_query, error_records)

            # 4. Bult Insert Clean Data
            if rows_inserted_updated > 0:
                upsert_query = """INSERT INTO silver_exchange_rates (base_currency, target_currency, rate, exchage_date)
                        VALUES %s ON CONFLICT (base_currency, target_currency, exchage_date)
                        DO UPDATE SET 
                        rate = EXCLUDED.rate, extracted_date = CURRENT_TIMESTAMP;"""
                # Prepare Clean ata tuple for bulk extraction
                clean_records = list(df_clean[['base_currency', 'tar_currency', 'rate_value', 'exchange_date']].itertuples(index = False, name = None))
                execute_values(cur, upsert_query, clean_records) 



            # # Initializing Audit Balance Control before processing
            # total_currencies_received = len(rates)
            # rows_inserted_updated = 0
            # rows_sent_to_exception = 0
            # logging.info(f"Starting SILVER Processing. Total Currencies to process {total_currencies_received}")
            # print(f"Processing {len(rates)} currency pairs for date {exchange_date}")
            
            # for tar_currency, rate_value in rates.items():
                
            #     # Checking Exceptional Case Currencies and pushing to respective Tables
            #     if rate_value <= 0:
            #         error_msg = f"Validation failed: Zero or Negative Exchange Rate Detected ({tar_currency} {rate_value})"
            #         print(f"Validation failed: Zero or Negative Exchange Rate Detected ({tar_currency} {rate_value})")
            #         error_query = """INSERT INTO silver_data_quality_errors (base_currency, target_currency, invalid_rate, error_reason)
            #         VALUES (%s, %s, %s, %s);"""
            #         cur.execute(error_query, (base, tar_currency, rate_value, error_msg))
            #         rows_sent_to_exception += 1
            #         continue
            #     #MAIN INSERTION for Rows After Validation
            #     query = "INSERT INTO silver_exchange_rates (base_currency, target_currency, rate, exchage_date) " \
            #     "VALUES (%s, %s, %s, %s) ON CONFLICT (base_currency, target_currency, exchage_date)" \
            #     " DO UPDATE SET " \
            #     "rate = EXCLUDED.rate, extracted_date = CURRENT_TIMESTAMP;"
            #     cur.execute(query, (base, tar_currency, rate_value, exchange_date))
                conn.commit()
               #rows_inserted_updated += 1
            print("Silver Layer executed successfully with all rates!")
            #Account Balance Sheet After Processing
            total_rows = (rows_inserted_updated + rows_sent_to_exception)
            balance_sheet = f"""
                ============================================================
                AUDIT BALANCE CONTROL (ABC) SHEET
                ============================================================
                Input (BRONZE SOURCE COUNT) : {total_currencies_received}
                Output (Silver Main Table) : {rows_inserted_updated}
                Output (Silver Exception Table) : {rows_sent_to_exception}
                Total Recounciled SUM : {total_rows}
                ============================================================
            """    
            print(balance_sheet)
            logger.info(balance_sheet)
            if(total_currencies_received == total_rows):
                success_msg = "RECOUNCILATION SUCCESSFULL : All rows accounted for Math Balance Successfully."
                print(success_msg)
                logger.info(success_msg)
            else:
                failure_msg = "RECOUNCILATION FAILED : Row count mismatch. Data Leakage Detected."
                print(failure_msg)
                logger.info(failure_msg)
    except Exception as e:
        conn.rollback()
        print(f"Silver processing failed: {e}")
        logger.error(f"Silver processing failed: {e}")
        raise e

    finally:
        conn.close()

if __name__ == "__main__":
    process_bronze_to_silver()