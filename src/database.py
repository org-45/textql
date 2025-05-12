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
        """Initialize the database connection pool and extensions."""
        try:
            self._conn = await asyncpg.create_pool(
                user=self.pg_user, password=self.pg_password, database=self.pg_db,
                host=self.pg_host, port=self.pg_port, min_size=10, max_size=50 
            )
            logger.info("PostgreSQL connection pool created successfully")

            # set up extensions
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    await conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
                    await conn.execute('CREATE EXTENSION IF NOT EXISTS vector;')

            #setup embeddings table
            await self.create_embedding_table("text_embeddings")

            logger.info("Database extensions initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database connection or extensions: {str(e)}")
            raise

    async def close_database_execution(self):
        """Close the database connection pool."""
        try:
            if self._conn:
                await self._conn.close()
                logger.info("Connection closed to PG")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")
            raise

    async def execute_query(self, query: str) -> Tuple[List[str], List[List[Any]]]:
        """Execute a SQL query and return column names and results with a timeout."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    results = await asyncio.wait_for(conn.fetch(query), timeout=SQL_EXECUTION_TIMEOUT)
                    if results:
                        column_names = list(results[0].keys())
                        return column_names, [list(row.values()) for row in results]
                    return [], []
        except asyncio.TimeoutError:
            logger.error(f"Query execution timed out after {SQL_EXECUTION_TIMEOUT} seconds: '{query}'")
            raise HTTPException(status_code=504, detail=f"Query execution timed out after {SQL_EXECUTION_TIMEOUT} seconds")
        except Exception as e:
            logger.error(f"Error executing query '{query}': {str(e)}")
            raise

    async def create_table(self, table_name: str, column_defs: str, primary_key: str = None, foreign_keys: list = None, partition_by: str = None):
        """Create a table with specified column definitions, primary key, foreign keys, and optional partitioning."""
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
                    if partition_by:
                        sql += f") PARTITION BY {partition_by}"
                    else:
                        sql += ")"
                    sql += ";"
                    await conn.execute(sql)
                    logger.info(f"Created table '{table_name}' with primary key '{primary_key}' and partition '{partition_by}'")
        except Exception as e:
            logger.error(f"Error creating table '{table_name}': {str(e)}")
            raise

    async def create_monthly_partitions(self, table_name: str):
        """Create monthly partitions for the flights table."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    for month in range(1, 13):
                        partition_name = f"{table_name}_2015_{month}"
                        await conn.execute(f"""
                            CREATE TABLE IF NOT EXISTS {partition_name} 
                            PARTITION OF {table_name} 
                            FOR VALUES IN ({month});
                        """)
                    logger.info(f"Created monthly partitions for table '{table_name}'")
        except Exception as e:
            logger.error(f"Error creating partitions for table '{table_name}': {str(e)}")
            raise

    async def import_csv(self, table_name: str, csv_file: str, primary_key: str = None, foreign_keys: list = None, partition_by:str = None):
        """Import CSV data into a table with rollback on failure, using temp table and partitioning for flights."""
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
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

                            #composite PRIMARY_KEY
                            if table_name == 'flights' and partition_by and "LIST (month)" in partition_by:
                                sql = f"CREATE TABLE {table_name} (unique_id UUID DEFAULT uuid_generate_v4(), {column_defs}, PRIMARY KEY (unique_id, month)"
                            else:
                                sql = f"CREATE TABLE {table_name} (unique_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), {column_defs}"
                            
                            if foreign_keys:
                                fk_constraints = [f"FOREIGN KEY (\"{fk['column'].lower()}\") REFERENCES {fk['references']}" for fk in foreign_keys]
                                sql += ", " + ", ".join(fk_constraints)
                            if partition_by:
                                sql += f") PARTITION BY {partition_by}"
                            else:
                                sql += ")"
                            sql += ";"
                            await conn.execute(sql)
                            
                            if table_name == 'flights' and partition_by and "LIST (month)" in partition_by:
                                # create partitions for months 1-12
                                for month in range(1, 13):
                                    partition_name = f"{table_name}_{month}"
                                    partition_sql = f"""
                                        CREATE TABLE IF NOT EXISTS {partition_name} 
                                        PARTITION OF {table_name}
                                        FOR VALUES IN ({month});
                                    """
                                    await conn.execute(partition_sql)
                                    
                                    # create indexes for each partition
                                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{partition_name}_origin ON {partition_name} (origin_airport);")
                                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{partition_name}_destination ON {partition_name} (destination_airport);")
                                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{partition_name}_airline ON {partition_name} (airline);")
                                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{partition_name}_departure_delay ON {partition_name} (departure_delay);")
                                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{partition_name}_arrival_delay ON {partition_name} (arrival_delay);")
                            
                            # insert data from temp table to partitioned table
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

                    # create indexes after table creation and data import
                    if table_name == "airlines":
                        await conn.execute("CREATE INDEX IF NOT EXISTS idx_airlines_iata ON airlines (iata_code);")
                    elif table_name == "airports":
                        await conn.execute("CREATE INDEX IF NOT EXISTS idx_airports_iata ON airports (iata_code);")
                        await conn.execute("CREATE INDEX IF NOT EXISTS idx_airports_state ON airports (state);")
                        await conn.execute("CREATE INDEX IF NOT EXISTS idx_airports_country ON airports (country);")
                    elif table_name == "flights":
                        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_flights_origin ON flights (origin_airport);")
                        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_flights_destination ON flights (destination_airport);")
                        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_flights_airline ON flights (airline);")
                        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_flights_departure_delay ON flights (departure_delay);")
                        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_flights_arrival_delay ON flights (arrival_delay);")
                    elif table_name == "text_embeddings":
                        await conn.execute("CREATE INDEX IF NOT EXISTS idx_text_embeddings ON text_embeddings USING ivfflat (embedding vector_l2_ops);")
            logger.info(f"Imported CSV data into table '{table_name}' from '{csv_file}' with indexes created")
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

    async def get_similar_rows(self, query_embedding: str, num_of_rows: int, offset: int = 0) -> List[Any]:
        """Retrieve similar rows based on vector embedding similarity with pagination."""
        sql = f"""
        SELECT table_name, row_data, embedding <=> CAST($1 AS vector) AS similarity
        FROM text_embeddings
        ORDER BY similarity ASC
        LIMIT $2 OFFSET $3
        """
        try:
            async with self._conn.acquire() as conn:
                async with conn.transaction():
                    results = await conn.fetch(sql, query_embedding, num_of_rows, offset)
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
                        AND table_name NOT IN ('feedback', 'text_embeddings')
                        AND table_name NOT LIKE 'flights_%'
                        OR table_name = 'flights';             
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
