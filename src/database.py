from typing import Tuple, List, Any
import asyncpg
import logging
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, pg_user: str, pg_password: str, pg_db: str, pg_host: str = "localhost", pg_port: int = 5432):
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_db = pg_db
        self.pg_host = pg_host
        self.pg_port = pg_port
        self._pool = None

    # initialize the postgresql db for execution
    async def initialize_database_execution(self):
        """Initialize the database with required tables."""
        try:
            self._pool = await asyncpg.create_pool(
                user=self.pg_user,
                password=self.pg_password,
                database=self.pg_db,
                host=self.pg_host,
                port=self.pg_port,
                min_size=10,
                max_size=20
            )

            logger.info("PostgreSQL connection pool created successfully") #Set and load logger to the db

        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
    async def execute_query(
        self,
        query: str
    ) -> Tuple[List[str], List[List[Any]]]:
        """Execute a SQL query and return column names and results."""
        try:
            async with self._pool.acquire() as db:
                async with db.transaction():
                    results = await db.fetch(query)
                    if results:
                        column_names = list(results[0].keys())
                    else:
                        column_names = []
                    return column_names, [list(row.values()) for row in results]
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    # closes postgresql db connection
    async def close_database_execution(self):
        """Close the database connection."""
        try:
            if self._pool:
                await self._pool.close()
                logger.info(f"Connection pool closed to PG")
            else:
                logger.info(f"There are no connections to close in the stack or to pool")
        except Exception as e:
            logger.error(f"Connection errored: {e}")