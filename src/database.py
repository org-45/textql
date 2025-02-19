from typing import Tuple, List, Any
import aiosqlite
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str = "textql.db"):
        self.db_path = db_path
        self.connection = None

    # initialize the sqlite db for execution
    async def initialize_database_execution(self):
        """Initialize the database with required tables."""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # add your database initialization code here
                await db.commit()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
    #takes query and executes it against sqlite
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

    # closes sqlite db connection
    async def close_database_execution(self):
        """Close the database connection."""
        if self.connection:
            await self.connection.close()
            self.connection = None

