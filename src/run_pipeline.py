import os
import sys
import logging

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True) 

# Configuring logging once for the entire application
logging.basicConfig(
    level= logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join("logs", "pipeline.log")),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("run_pipeline")

def run_master_pipeline():
    logger.info("===============================================================")
    logger.info("STARTING EXCHANGE RATE DATA PIPELINE")
    logger.info("===============================================================")

    # PHASE 1/3: BRONZE :: Triggering Data Ingestion............
    try: 
        # Importing Main function from extract_data.py
        logger.info("BRONZE Phase Started")
        from extract_data import main as run_bronze
        run_bronze()
        logger.info("BRONZE Phase Completed Successfully")
    except Exception as e:
        logger.error(f"Critical Exception during BRONZE Phase:: {e}")
        logger.info("Pipeline halted to prevent corrupting downstream data")
        sys.exit(1)

    # PHASE 2/3: SILVER :: Run SILVER Processing Phase............
    try:    
        logger.info("SILVER Phase Started")
        from process_silver import process_bronze_to_silver as run_silver
        run_silver()
        logger.info("SILVER Phase Completed Successfully")
    except Exception as e:
        logger.error(f"Critical Exception during SILVER Phase:: {e}")
        logger.info("Pipeline halted to prevent corrupting downstream data")
        sys.exit(1)

    # PHASE 3/3: SILVER :: Run GOLD Processing Phase............
    try:    
        logger.info("GOLD Phase Started")
        from process_gold import process_silever_to_gold as run_gold
        run_gold()
        logger.info("GOLD Phase Completed Successfully")
    except Exception as e:
        logger.error(f"Critical Exception during GOLD Phase:: {e}")
        logger.info("Pipeline halted to prevent corrupting downstream data")
        sys.exit(1)

    logger.info("===============================================================")
    logger.info("PIPELINE RUN COMPLETED SUCCESSFULLY WITHOUT ERRORS")
    logger.info("===============================================================")

if __name__ =="__main__":
     run_master_pipeline()