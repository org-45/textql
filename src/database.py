from typing import Tuple, List, Any
import aiosqlite
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str = "textql.db"):
        self.db_path = db_path
        self.connection = None

    async def initialize_database(self):
        """Initialize the database with required tables."""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # add your database initialization code here
                await db.commit()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise

    async def get_connection(self):
        """Get a database connection."""
        if not self.connection:
            self.connection = await aiosqlite.connect(self.db_path)
        return self.connection

    async def close(self):
        """Close the database connection."""
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def execute_query(
        self,
        query: str
    ) -> Tuple[List[str], List[List[Any]]]:
        """Execute a SQL query and return column names and results."""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(query) as cursor:
                    rows = await cursor.fetchall()
                    if cursor.description:
                        column_names = [desc[0] for desc in cursor.description]
                    else:
                        column_names = []
                    
                    results = [list(row) for row in rows]
                    return column_names, results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    async def load_queries(self, filepath: str = "data/queries.json") -> List[dict]:
        """Load and validate queries from JSON file."""
        try:
            path = Path(filepath)
            if not path.exists():
                logger.warning(f"Queries file not found: {filepath}")
                return []

            with open(path, "r") as file:
                data = json.load(file)
                
                # Validate query format
                for entry in data:
                    if not isinstance(entry, dict) or \
                       "sql" not in entry or \
                       "description" not in entry:
                        logger.error("Invalid format in queries.json")
                        return []
                        
                return data
        except Exception as e:
            logger.error(f"Error loading queries: {str(e)}")
            return []