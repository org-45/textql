from typing import Tuple, List, Any
import asyncpg
import logging
import csv
import asyncio
from src.config.tables import COLUMN_TYPE_MAPPING
from fastapi import HTTPException
from src.config.settings import SQL_EXECUTION_TIMEOUT, VECTOR_ROWS_IN_PROMPT

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, pg_user: str, pg_password: str, pg_db: str, pg_host: str = "localhost", pg_port: int = 5432):
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_db = pg_db
        self.pg_host = pg_host
        self.pg_port = pg_port
        self._conn = None

    async def initialize_database_execution(self):
        """Initialize the database connection pool."""
        try:
            self._conn = await asyncpg.create_pool(
                user=self.pg_user, password=self.pg_password, database=self.pg_db,
                host=self.pg_host, port=self.pg_port
            )
            logger.info("PostgreSQL connection created successfully")
        except Exception as e:
            logger.error(f"Error initializing database connection: {str(e)}")
            raise

    async def close_database_execution(self):
        """Close the database connection."""
        try:
            if self._conn:
                await self._conn.close()
                logger.info("Connection closed to PG")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")
            raise

    async def execute_query(self, query: str) -> Tuple[List[str], List[List[Any]]]:
        """Execute a SQL query and return column names and results with a 10-second timeout."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    results = await asyncio.wait_for(conn.fetch(query), timeout=SQL_EXECUTION_TIMEOUT)
                    if results:
                        column_names = list(results[0].keys())
                        return column_names, [list(row.values()) for row in results]
                    return [], []
        except asyncio.TimeoutError:
            logger.error(f"Query execution timed out after 10 seconds: '{query}'")
            raise HTTPException(status_code=504, detail="Query execution timed out after 10 seconds")
        except Exception as e:
            logger.error(f"Error executing query '{query}': {str(e)}")
            raise

    async def create_table(self, table_name: str, column_defs: str, primary_key: str = None, foreign_keys: list = None):
        """Create a table with specified column definitions, primary key, and foreign keys."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs}"
                    if primary_key == "unique_id":
                        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (unique_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), {column_defs}"
                    elif primary_key:
                        sql += f", PRIMARY KEY (\"{primary_key.lower()}\")"
                    if foreign_keys:
                        fk_constraints = [f"FOREIGN KEY (\"{fk['column'].lower()}\") REFERENCES {fk['references']}" for fk in foreign_keys]
                        sql += ", " + ", ".join(fk_constraints)
                    sql += ");"
                    await conn.execute(sql)
                    logger.info(f"Created table '{table_name}' with primary key '{primary_key}' and foreign keys")
        except Exception as e:
            logger.error(f"Error creating table '{table_name}': {str(e)}")
            raise

    async def import_csv(self, table_name: str, csv_file: str, primary_key: str = None, foreign_keys: list = None):
        """Import CSV data into a table with rollback on failure, using temp table for flights."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    #open file in binary mode
                    with open(csv_file, "rb") as file:
                        reader = csv.reader(file.read().decode('utf-8').splitlines())
                        header = [col.strip().lower() for col in next(reader)]
                        mapping = COLUMN_TYPE_MAPPING.get(table_name, {})
                        column_defs = ", ".join([f"{col} {mapping.get(col, 'TEXT')}" for col in header])
                        column_names = [col for col in header]
                        quoted_column_names = [f'"{col}"' for col in header]

                        if primary_key == "unique_id":
                            temp_table = f"{table_name}_temp"
                            await conn.execute(f"CREATE TABLE IF NOT EXISTS {temp_table} ({column_defs});")
                            file.seek(0)
                            await conn.copy_to_table(
                                temp_table,
                                source=file,
                                columns=column_names,
                                format='csv',
                                header=True,
                                delimiter=','
                            )
                            await conn.execute(f"DROP TABLE IF EXISTS {table_name};")
                            sql = f"CREATE TABLE {table_name} (unique_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), {column_defs}"
                            if foreign_keys:
                                fk_constraints = [f"FOREIGN KEY (\"{fk['column'].lower()}\") REFERENCES {fk['references']}" for fk in foreign_keys]
                                sql += ", " + ", ".join(fk_constraints)
                            sql += ");"
                            await conn.execute(sql)
                            insert_sql = f"INSERT INTO {table_name} ({', '.join(quoted_column_names)}) SELECT {', '.join(quoted_column_names)} FROM {temp_table};"
                            await conn.execute(insert_sql)
                            await conn.execute(f"DROP TABLE {temp_table};")
                        else:
                            await self.create_table(table_name, column_defs, primary_key, foreign_keys)
                            file.seek(0)
                            await conn.copy_to_table(
                                table_name,
                                source=file,
                                columns=column_names,
                                format='csv',
                                header=True,
                                delimiter=','
                            )
            logger.info(f"Imported CSV data into table '{table_name}' from '{csv_file}'")
        except Exception as e:
            logger.error(f"Error importing CSV into table '{table_name}' from '{csv_file}': {str(e)}")
            raise

    async def create_embedding_table(self, table_name: str):
        """Create a table for vector embeddings with rollback on failure."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255),
            row_data TEXT,
            embedding vector(384)
        );
        """
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(sql)
                    logger.info(f"Created embedding table '{table_name}'")
        except Exception as e:
            logger.error(f"Error creating embedding table '{table_name}': {str(e)}")
            raise

    async def insert_embeddings(self, table_name: str, rows: List[Tuple[str, str, list]]):
        """Insert embeddings with rollback on failure."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    formatted_rows = [(r[0], r[1], '[' + ','.join(map(str, r[2])) + ']') for r in rows]
                    await conn.executemany(
                        "INSERT INTO text_embeddings (table_name, row_data, embedding) VALUES ($1, $2, $3)",
                        formatted_rows
                    )
                    logger.info(f"Inserted {len(rows)} embeddings into '{table_name}'")
        except Exception as e:
            logger.error(f"Error inserting embeddings into '{table_name}': {str(e)}")
            raise

    async def get_similar_rows(self, query_embedding: str, num_of_rows: int) -> List[Any]:
        """Retrieve similar rows based on vector embedding similarity."""
        sql = f"""
        SELECT table_name, row_data, embedding <#> CAST($1 AS vector) AS similarity
        FROM text_embeddings
        ORDER BY similarity ASC
        LIMIT $2
        """
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    results = await conn.fetch(sql, query_embedding, num_of_rows)
                    return results
        except Exception as e:
            logger.error(f"Error retrieving similar rows: {str(e)}")
            raise

    async def store_feedback(self, natural_language: str, sql_query: str, feedback: str, corrected_sql: str = None):
        """Store user feedback with rollback on failure based on new schema."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("""
                    CREATE TABLE IF NOT EXISTS feedback (
                        id SERIAL PRIMARY KEY,
                        natural_language TEXT,
                        correct_sql_query TEXT,
                        incorrect_sql_query TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """)
                    if feedback == "yes":
                        await conn.execute(
                            "INSERT INTO feedback (natural_language, correct_sql_query, incorrect_sql_query) VALUES ($1, $2, $3)",
                            natural_language, sql_query, None
                        )
                    elif feedback == "no" and corrected_sql:
                        await conn.execute(
                            "INSERT INTO feedback (natural_language, correct_sql_query, incorrect_sql_query) VALUES ($1, $2, $3)",
                            natural_language, corrected_sql, sql_query
                        )
                    elif feedback == "no":
                        await conn.execute(
                            "INSERT INTO feedback (natural_language, correct_sql_query, incorrect_sql_query) VALUES ($1, $2, $3)",
                            natural_language, None, sql_query
                        )
                    else:
                        raise ValueError("Invalid feedback value")
                    logger.info("Stored feedback for query")
        except Exception as e:
            logger.error(f"Error storing feedback: {str(e)}")
            raise

    async def get_schema(self) -> dict:
        """Dynamically fetch schema information."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    tables = await conn.fetch("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        AND table_name NOT IN ('feedback', 'text_embeddings');
                    """)
                    schema = {"tables": {}}
                    for table in tables:
                        columns = await conn.fetch(
                            "SELECT column_name FROM information_schema.columns WHERE table_name = $1;",
                            table["table_name"]
                        )
                        schema["tables"][table["table_name"]] = [col["column_name"] for col in columns]
                    return schema
        except Exception as e:
            logger.error(f"Error fetching schema: {str(e)}")
            raise

    async def get_sample_data(self, table_name: str, limit: int = 5) -> List[dict]:
        """Fetch sample data from a table."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    results = await conn.fetch(f"SELECT * FROM {table_name} LIMIT $1;", limit)
                    column_names = list(results[0].keys()) if results else []
                    return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error fetching sample data from '{table_name}': {str(e)}")
            raise