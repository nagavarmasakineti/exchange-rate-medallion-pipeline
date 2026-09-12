import os
import logging
from dotenv import load_dotenv
from datetime import datetime
from db import get_db_connection

logger = logging.getLogger("run_pipeline")

#Load Dot env
load_dotenv()

def process_silever_to_gold():
    logger.info("***** process_silver_to_gold *****")
    # Establish Data Connection Here
    conn = get_db_connection()
    cursor = conn.cursor()
    logger.info("Successfully Connected to Database") 
    
    try:
        logger.info("Joining Silver data with Dimensions and calculating rolling metrics...")
        transformation_query = """ with silver_calculated as (
            Select exchage_date, target_currency, rate, 
            avg(rate) OVER(PARTITION BY target_currency ORDER BY exchage_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg
            from silver_exchange_rates 
            where exchage_date = current_date)
            select d.date_key, c.currency_key, s.rate, s.rolling_7day_avg
            from silver_calculated s
            JOIN dim_currencies c ON c.currency_code = s.target_currency
            JOIN dim_dates d ON d.full_date = s.exchage_date
        """
        cursor.execute(transformation_query)
        gold_records = cursor.fetchall()

        if not gold_records:
            logger.warning("No matching records found across Silver or dimension tables")
            cursor.close()
            conn.close()
            return
        # 4. IDEMPOTENT WRITE: Upsert into fact_exchange_rates
        logger.info(f"Upserting {len(gold_records)} rows into fact_exchange_rates...")
        
        upsert_query = """
            INSERT INTO fact_exchange_rates (date_key, currency_key, exchange_rate, rolling_7day_avg, created_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (date_key, currency_key) 
            DO UPDATE SET 
                exchange_rate = EXCLUDED.exchange_rate,
                rolling_7day_avg = EXCLUDED.rolling_7day_avg;
        """

        # Using psycopg2's execute_batch to efficiently insert rows
        from psycopg2 import extras
        extras.execute_batch(cursor, upsert_query, gold_records)

        # 5. Commit changes safely
        conn.commit()
        logger.info("** Gold Star Schema update completed successfully! **")
    except Exception as e:
        conn.rollback()
        print(f"Gold processing failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    process_silever_to_gold()
