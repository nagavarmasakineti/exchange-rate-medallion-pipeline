import os
import requests
import psycopg2
import logging
from dotenv import load_dotenv

# Configure the logging framework
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

#load variables from dotenv
load_dotenv()

DB_CONFIG = {
    "dbname" : os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password" : os.getenv("DB_PASSWORD"),
    "host" : os.getenv("DB_HOST"),
    "port" : "5432"
}

def get_exchange_rate(base_currency, api_key):
    #url = "https://v6.exchangerate-api.com/v6/ce72099bbbd562c43df5409c/latest/USD"
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{base_currency}"
    response = requests.get(url, timeout=20, verify=True)
    response.raise_for_status()
    return response.text

def save_to_bronze(raw_payload):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            query = "INSERT INTO  bronze_exchange_rates (raw_payload) VALUES (%s)"
            cur.execute(query, (raw_payload,))
        conn.commit()
        logging.info("Raw payload successfully loaded")
        print("Raw Data saved successfully")
    except Exception as e:
        logging.error(f"Failed to write to Bronze: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()

def validate_and_clean_data(base, target, rate):
    #Rule 1: CURRENCIES SHOULD BE UPPERCASE and 3-letter strings
    if(len(base)!= 3 or len(target)!=3):
        logging.warning(f"Validation Failed: Invalid Currency Code format: {base} ---> {target}")
        return False
    #Rule 2: Rate should be greater than zero
    if(rate <= 0):
        logging.warning(f"Validation Failed: Impossible exchange rate detected: {rate}")
        return False
    return True



def save_rate_to_db(base, target, rate):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            query = "INSERT INTO  exchange_rates (base_currecncy, target_currency, rate) VALUES (%s, %s, %s)" \
            "ON CONFLICT (base_currecncy, target_currency, (CAST(extracted_at AS DATE))) DO UPDATE " \
            "SET rate = EXCLUDED.rate, extracted_at = CURRENT_TIMESTAMP"
            cur.execute(query, (base, target, rate))
        conn.commit()
    
    finally:
        conn.close()

# "Main Entry Point"
def main():
    try:
        API_KEY = os.getenv("API_KEY")
        logging.info("Pipeline execution started.")

        # Fetch Raw payload string
        raw_payload_string = get_exchange_rate("USD", API_KEY)

        # Save to BRONZE table complete raw data
        save_to_bronze(raw_payload_string)
        logging.info("Data Saved Successfully to BRONZE")
        print("Data Saved Successfully to BRONZE")

        # Now parse it to downstream SILVER LAYER
        import json
        data = json.loads(raw_payload_string)
        base = data['base_code']
        rate = data['conversion_rates']['EUR']

        #Validation Gate
        if validate_and_clean_data(base, "EUR", rate):
            save_rate_to_db(base, "EUR", rate)
            logging.info("pipeline executed successfully")
            print("Pipeline execution successfull.")
        else:
            logging.error("Pipline Stopped: Data Failed Validation Quality Checks.")
            print("Pipline Stopped: Data Failed Validation Quality Checks.")

    except Exception as e:
        logging.error(f"Pipeling failed: {str(e)}", exc_info=True)
        print(f"Pipeline failed: {e}")