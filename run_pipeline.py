import os
import sys
import logging
from datetime import datetime

# Set up logs to track our automated runs
logging.basicConfig(
    level= logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def run_master_pipeline():
    logging.info("===============================================================")
    logging.info("STARTING EXCHANGE RATE DATA PIPELINE")
    logging.info("===============================================================")

    # PHASE 1/3: BRONZE :: Triggering Data Ingestion............
    try: 
        # Importing Main function from extract_data.py
        logging.info("BRONZE Phase Started")
        from extract_data import main as run_bronze
        run_bronze()
        logging.info("BRONZE Phase Completed Successfully")
    except Exception as e:
        logging.error(f"Critical Exception during BRONZE Phase:: {e}")
        logging.info("Pipeline halted to prevent corrupting downstream data")
        sys.exit(1)

    # PHASE 2/3: SILVER :: Run SILVER Processing Phase............
    try:    
        logging.info("SILVER Phase Started")
        from process_silver import process_bronze_to_silver as run_silver
        run_silver()
        logging.info("SILVER Phase Completed Successfully")
    except Exception as e:
        logging.error(f"Critical Exception during SILVER Phase:: {e}")
        logging.info("Pipeline halted to prevent corrupting downstream data")
        sys.exit(1)

    # PHASE 3/3: SILVER :: Run GOLD Processing Phase............
    try:    
        logging.info("GOLD Phase Started")
        from process_gold import process_silever_to_gold as run_gold
        run_gold()
        logging.info("GOLD Phase Completed Successfully")
    except Exception as e:
        logging.error(f"Critical Exception during GOLD Phase:: {e}")
        logging.info("Pipeline halted to prevent corrupting downstream data")
        sys.exit(1)

    logging.info("===============================================================")
    logging.info("PIPELINE RUN COMPLETED SUCCESSFULLY WITHOUT ERRORS")
    logging.info("===============================================================")

if __name__ =="__main__":
     run_master_pipeline()