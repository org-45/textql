from typing import Tuple, List, Any
import psycopg2
from psycopg2.extras import execute_batch
import logging
import csv
from src.config.tables import COLUMN_TYPE_MAPPING

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, pg_user: str, pg_password: str, pg_db: str, pg_host: str = "localhost", pg_port: int = 5432):
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_db = pg_db
        self.pg_host = pg_host
        self.pg_port = pg_port
        self._conn = None

    def initialize_database_execution(self):
        """Initialize the database connection."""
        try:
            self._conn = psycopg2.connect(
                user=self.pg_user, password=self.pg_password, dbname=self.pg_db,
                host=self.pg_host, port=self.pg_port
            )
            logger.info("PostgreSQL connection created successfully")
        except Exception as e:
            logger.error(f"Error initializing database connection: {str(e)}")
            raise

    def close_database_execution(self):
        """Close the database connection."""
        try:
            if self._conn:
                self._conn.close()
                logger.info("Connection closed to PG")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")
            raise

    def execute_query(self, query: str) -> Tuple[List[str], List[List[Any]]]:
        """Execute a SQL query and return column names and results."""
        cursor = None
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            if cursor.description:
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                return column_names, [list(row) for row in results]
            self._conn.commit()
            return [], []
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Error executing query '{query}': {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create_table(self, table_name: str, column_defs: str, primary_key: str = None, foreign_keys: list = None):
        """Create a table with specified column definitions, primary key, and foreign keys."""
        cursor = None
        try:
            cursor = self._conn.cursor()
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs}"
            if primary_key == "unique_id":
                sql = f"CREATE TABLE IF NOT EXISTS {table_name} (unique_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), {column_defs}"
            elif primary_key:
                sql += f", PRIMARY KEY (\"{primary_key.lower()}\")"
            if foreign_keys:
                fk_constraints = [f"FOREIGN KEY (\"{fk['column'].lower()}\") REFERENCES {fk['references']}" for fk in foreign_keys]
                sql += ", " + ", ".join(fk_constraints)
            sql += ");"
            cursor.execute(sql)
            self._conn.commit()
            logger.info(f"Created table '{table_name}' with primary key '{primary_key}' and foreign keys")
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Error creating table '{table_name}': {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def import_csv(self, table_name: str, csv_file: str, primary_key: str = None, foreign_keys: list = None):
        """Import CSV data into a table with rollback on failure, using temp table for flights."""
        cursor = None
        try:
            cursor = self._conn.cursor()
            with open(csv_file, "r", encoding="utf-8") as file:
                reader = csv.reader(file)
                header = [col.strip().lower() for col in next(reader)]  
                mapping = COLUMN_TYPE_MAPPING.get(table_name, {})
                column_defs = ", ".join([f"{col} {mapping.get(col, 'TEXT')}" for col in header])
                column_names = [f'"{col}"' for col in header]

                if primary_key == "unique_id":  
                    temp_table = f"{table_name}_temp"
                    
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS {temp_table} ({column_defs});")
                    self._conn.commit()

                    
                    file.seek(0)  
                    cursor.copy_expert(f"COPY {temp_table} FROM STDIN WITH CSV HEADER DELIMITER ','", file)

                    
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                    sql = f"CREATE TABLE {table_name} (unique_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), {column_defs}"
                    if foreign_keys:
                        fk_constraints = [f"FOREIGN KEY (\"{fk['column'].lower()}\") REFERENCES {fk['references']}" for fk in foreign_keys]
                        sql += ", " + ", ".join(fk_constraints)
                    sql += ");"
                    cursor.execute(sql)

                    
                    cursor.execute(f"INSERT INTO {table_name} ({', '.join(column_names)}) SELECT {', '.join(column_names)} FROM {temp_table};")
                    cursor.execute(f"DROP TABLE {temp_table};")
                else:
                    
                    self.create_table(table_name, column_defs, primary_key, foreign_keys)
                    
                    file.seek(0)  
                    cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", file)

            self._conn.commit()
            logger.info(f"Imported CSV data into table '{table_name}' from '{csv_file}'")
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Error importing CSV into table '{table_name}' from '{csv_file}': {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def create_embedding_table(self, table_name: str):
        """Create a table for vector embeddings with rollback on failure."""
        cursor = None
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255),
            row_data TEXT,
            embedding vector(384)
        );
        """
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            self._conn.commit()
            logger.info(f"Created embedding table '{table_name}'")
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Error creating embedding table '{table_name}': {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def insert_embeddings(self, table_name: str, rows: List[Tuple[str, str, list]]):
        """Insert embeddings with rollback on failure."""
        cursor = None
        try:
            cursor = self._conn.cursor()
            execute_batch(cursor, "INSERT INTO text_embeddings (table_name, row_data, embedding) VALUES (%s, %s, %s)", rows)
            self._conn.commit()
            logger.info(f"Inserted {len(rows)} embeddings into '{table_name}'")
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Error inserting embeddings into '{table_name}': {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_similar_rows(self, query_embedding: str, num_of_rows: int = 5) -> List[Any]:
        """Retrieve similar rows based on vector embedding similarity."""
        cursor = None
        sql = f"""
        SELECT table_name, row_data, embedding <
        FROM text_embeddings
        ORDER BY similarity ASC
        LIMIT %s
        """
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, (query_embedding, num_of_rows))
            results = cursor.fetchall()
            return results
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Error retrieving similar rows: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def store_feedback(self, natural_language: str, sql_query: str, feedback: str, corrected_sql: str = None):
        """Store user feedback with rollback on failure based on new schema."""
        cursor = None
        try:
            cursor = self._conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id SERIAL PRIMARY KEY,
                natural_language TEXT,
                correct_sql_query TEXT,
                incorrect_sql_query TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
            if feedback == "yes":
                cursor.execute(
                    "INSERT INTO feedback (natural_language, correct_sql_query, incorrect_sql_query) VALUES (%s, %s, %s)",
                    (natural_language, sql_query, None)
                )
            elif feedback == "no" and corrected_sql:
                cursor.execute(
                    "INSERT INTO feedback (natural_language, correct_sql_query, incorrect_sql_query) VALUES (%s, %s, %s)",
                    (natural_language, corrected_sql, sql_query)
                )
            elif feedback == "no":
                cursor.execute(
                    "INSERT INTO feedback (natural_language, correct_sql_query, incorrect_sql_query) VALUES (%s, %s, %s)",
                    (natural_language, None, sql_query)
                )
            else:
                raise ValueError("Invalid feedback value")
            self._conn.commit()
            logger.info("Stored feedback for query")
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Error storing feedback: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_schema(self) -> dict:
        """Dynamically fetch schema information."""
        cursor = None
        try:
            cursor = self._conn.cursor()
            cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
                                AND table_name NOT IN ('feedback','text_embeddings');
                           """)
            tables = cursor.fetchall()
            schema = {"tables": {}}
            for table in tables:
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table[0],))
                columns = cursor.fetchall()
                schema["tables"][table[0]] = [col[0] for col in columns]
            return schema
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Error fetching schema: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_sample_data(self, table_name: str, limit: int = 5) -> List[dict]:
        """Fetch sample data from a table."""
        cursor = None
        try:
            cursor = self._conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT %s;", (limit,))
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            return [dict(zip(column_names, row)) for row in results]
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Error fetching sample data from '{table_name}': {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()