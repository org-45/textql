import os
import logging
import asyncio
from sentence_transformers import SentenceTransformer
import json
import csv
from src.database import DatabaseManager
from src.config.settings import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT
from src.config.tables import TABLES_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
DATA_DIR = "data"

async def initialize_database():
    """Initialize database with CSV data and embeddings."""
    db = DatabaseManager(POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT)
    await db.initialize_database_execution()
    try:
        # ensure extensions
        await db.execute_query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
        await db.execute_query("CREATE EXTENSION IF NOT EXISTS vector;")

        # create embedding table
        await db.create_embedding_table("text_embeddings")

        # process CSV files using TABLES_CONFIG
        embed_model = SentenceTransformer("all-MiniLM-L6-v2")
        for config in TABLES_CONFIG:
            table_name = config['table_name']
            csv_path = os.path.join(DATA_DIR, config['csv_file'])
            primary_key = config['primary_key']
            foreign_keys = config.get('foreign_keys', [])

            logger.info(f"Importing {csv_path} into table '{table_name}'")
            await db.import_csv(table_name, csv_path, primary_key, foreign_keys)

            if table_name in ["airlines", "airports"]:
                logger.info(f"Generating embeddings for table '{table_name}'")
                with open(csv_path, "r", encoding="utf-8") as file:
                    csv_reader = csv.reader(file)
                    next(csv_reader)
                    row_data = [(table_name, json.dumps(row), embed_model.encode(json.dumps(row)).tolist()) for row in csv_reader]
                await db.insert_embeddings(table_name, row_data)
                logger.info(f"Inserted embeddings for '{table_name}'")

    except Exception as e:
        logger.error(f"Error during database initialization: {e}")
        raise
    finally:
        await db.close_database_execution()
        logger.info("Database initialization complete")

if __name__ == "__main__":
    asyncio.run(initialize_database())