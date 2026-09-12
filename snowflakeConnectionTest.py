import os
import json
import snowflake.connector

conn = snowflake.connector.connect(
    user = 'NSAKINETI',
    password = 'Nsakinetivarma@123',
    account = 'YFPIYNR-DR34391',
    warehouse = 'COMPUTE_WH',
    database = 'CURRENCY_DW',
    schema = 'BRONZE',
    role = 'ACCOUNTADMIN'
)

#Mocking API payload for testing
mock_api_json = {
    "base" : "USD",
    "date" : "11-07-2026",
    "rates":{
        "INR": 83.50,
        "AED" : 3.67,
        "EUR" : 0.92
    }
}

try:
    cursor = conn.cursor()
    cursor.execute("USE WAREHOUSE COMPUTE_WH;")
    json_string = json.dumps(mock_api_json);
    insert_query = f"INSERT INTO CURRENCY_DW.BRONZE.BRONZE_EXCHANGE_RATES (raw_payload) SELECT PARSE_JSON('{json_string}');"
    cursor.execute(insert_query)
    conn.commit()
    print (f"Successfully ingested raw json into snowflake Bronze Layer")
except Exception as e:
    print(f"Snowflake Connection is failed: {e}")
finally:
    cursor.close()
    conn.close()

