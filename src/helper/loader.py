import json
import logging
import random
from src.database import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def load_queries(filepath: str = "data/queries.json") -> list:
    """Loads a limited number of example queries from a JSON file."""
    try:
        with open(filepath, 'r') as f:
            queries = json.load(f)
            return random.sample(queries, min(2, len(queries)))
    except Exception as e:
        logger.error(f"Error loading queries: {e}")
        return []

async def load_schema_and_samples(db: DatabaseManager) -> dict:
    """Loads schema and sample data dynamically."""
    schema = await db.get_schema()
    for table in schema["tables"]:
        samples = await db.get_sample_data(table)
        schema["tables"][table] = {"columns": schema["tables"][table], "samples": [dict(row) for row in samples]}
    return schema