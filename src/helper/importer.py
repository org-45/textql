#imports data from csv, feeds it to tables and in vector index

import os
import psycopg2
from psycopg2.extras import execute_batch
import json
import logging
from sentence_transformers import SentenceTransformer
import csv

# configuration for setting up postgres db
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "vector_db")
DATA_DIR = "data"

# configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# define manual column type mappings for each table.
COLUMN_TYPE_MAPPING = {
    "airlines": {
        "iata_code": "TEXT",
        "airline": "TEXT"
    },
    "airports": {
        "iata_code": "TEXT",
        "airport": "TEXT",
        "city": "TEXT",
        "state": "TEXT",
        "country": "TEXT",
        "latitude": "DOUBLE PRECISION",
        "longitude": "DOUBLE PRECISION"
    },
    "flights": {
        "year": "INTEGER",
        "month": "INTEGER",
        "day": "INTEGER",
        "day_of_week": "INTEGER",
        "airline": "TEXT",
        "flight_number": "NUMERIC",
        "tail_number": "TEXT",
        "origin_airport": "TEXT",
        "destination_airport": "TEXT",
        "scheduled_departure": "TEXT",
        "departure_time": "TEXT",
        "departure_delay": "NUMERIC",
        "taxi_out": "NUMERIC",
        "wheels_off": "NUMERIC",
        "scheduled_time": "NUMERIC",
        "elapsed_time": "NUMERIC",
        "air_time": "NUMERIC",
        "distance": "NUMERIC",
        "wheels_on": "NUMERIC",
        "taxi_in": "NUMERIC",
        "scheduled_arrival": "TEXT",
        "arrival_time": "TEXT",
        "arrival_delay": "NUMERIC",
        "diverted": "NUMERIC",
        "cancelled": "NUMERIC",
        "cancellation_reason": "TEXT",
        "air_system_delay": "NUMERIC",
        "security_delay": "NUMERIC",
        "airline_delay": "NUMERIC",
        "late_aircraft_delay": "NUMERIC",
        "weather_delay": "NUMERIC"
    }
}


def ensure_extensions(conn):
    """Ensure required PostgreSQL extensions exist."""
    try:
        cursor = conn.cursor()
        cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        conn.commit()
        logger.info("Required extensions (uuid-ossp, vector) ensured.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error ensuring extensions: {e}")


def create_pg_table(conn, table_name, column_defs, primary_key=None):
    """
    Create a table in Postgres with specified column definitions and primary key.
    All column names should be in lowercase.
    """
    try:
        cursor = conn.cursor()
        
        if primary_key == "unique_id":
            id_def = 'unique_id UUID PRIMARY KEY DEFAULT uuid_generate_v4()'
            full_defs = id_def + (", " + column_defs if column_defs.strip() else "")
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({full_defs});"
        elif primary_key:
            pk_def = f', PRIMARY KEY ("{primary_key}")'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs}{pk_def});"
        else:
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs});"
        
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Successfully created table '{table_name}'")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating table '{table_name}': {e}")


def import_csv_to_pg(conn, table_name, csv_file, primary_key=None):
    """
    Import data from CSV file into PostgreSQL table.
    All CSV column names will be converted to lowercase and assigned to types
    based on the manual mapping in COLUMN_TYPE_MAPPING. If a column is missing from
    the mapping, it defaults to TEXT.
    
    For the flights table (primary_key == "unique_id"), a temporary table is used
    to import CSV data then re-create the table with a separate auto-generated UUID.
    """
    cursor = conn.cursor()
    try:
        with open(csv_file, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            header = next(reader)

            lower_header = [col.strip().lower() for col in header]

            # build column definitions based on mapping; default to TEXT if not in mapping.
            mapping = COLUMN_TYPE_MAPPING.get(table_name, {})
            column_defs_list = [
                f"{col} {mapping.get(col, 'TEXT')}" for col in lower_header
            ]
            csv_column_defs = ", ".join(column_defs_list)

            # build column names list for later use
            column_names = [f'"{col}"' for col in lower_header]
            
            if primary_key == "unique_id":
                # for flights table: use a temporary table to import CSV data and then add unique_id
                temp_table = f"{table_name}_temp"
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {temp_table} ({csv_column_defs});")
                conn.commit()
                
                with open(csv_file, "r", encoding="utf-8") as f:
                    # skip header row
                    next(csv.reader(f))
                    cursor.copy_expert(
                        sql=f"COPY {temp_table} FROM STDIN WITH CSV HEADER DELIMITER ','",
                        file=f
                    )
                
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                cursor.execute(f"""
                    CREATE TABLE {table_name} (
                        unique_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                        {csv_column_defs}
                    );
                """)
                cursor.execute(f"""
                    INSERT INTO {table_name} ({', '.join(column_names)})
                    SELECT {', '.join(column_names)} FROM {temp_table};
                """)
                cursor.execute(f"DROP TABLE {temp_table};")
            else:
                # for non-flights tables: use regular approach.
                if primary_key:
                    pk_col = primary_key.lower()
                    pk_def = f', PRIMARY KEY ("{pk_col}")'
                    sql_table = f"CREATE TABLE IF NOT EXISTS {table_name} ({csv_column_defs}{pk_def});"
                else:
                    sql_table = f"CREATE TABLE IF NOT EXISTS {table_name} ({csv_column_defs});"
                
                cursor.execute(sql_table)
                conn.commit()
                
                with open(csv_file, "r", encoding="utf-8") as f:
                    next(csv.reader(f))
                    cursor.copy_expert(
                        sql=f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','",
                        file=f
                    )
        
        conn.commit()
        logger.info(f"Successfully imported data into table '{table_name}' from '{csv_file}'")
        print(f"Successfully imported data into table '{table_name}' from '{csv_file}'")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error importing CSV into table '{table_name}': {e}")


def create_index(conn, table_name, column_name):
    """Create an index on the specified table column (column names in lowercase)."""
    index_name = f"idx_{table_name}_{column_name.lower()}"
    sql = f'CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ("{column_name.lower()}")'
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print(f"Index created on table '{table_name}' column '{column_name.lower()}'")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating index on table '{table_name}' column '{column_name.lower()}': {e}")


def create_pg_embedding_table(conn, table_name):
    """Create dedicated table for vector embeddings."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(255),
        row_data TEXT,
        embedding vector(384)
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Successfully created embedding table '{table_name}'")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating embedding table '{table_name}': {e}")


def initialize_database_import():
    """Initialize PostgreSQL database with CSV data and create embeddings.  
    All table and column names will be in lowercase.
    For flights table, a separate auto-generated UUID column ("unique_id") is used as primary key.
    For airlines and airports, the primary key is assigned to "iata_code" (in lowercase).
    """
    pg_conn = None
    try:
        logger.info("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB,
            host="localhost",
            port="5432"
        )
        logger.info("Connected to PostgreSQL successfully")
        
        #1. ensure extensions
        ensure_extensions(pg_conn)
        
        # 2. load sentence transformer model
        logger.info("Loading SentenceTransformer model...")
        embed_model = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("SentenceTransformer model loaded")
        
        # 3. vector embeddings will be created
        embedding_table = "text_embeddings"
        create_pg_embedding_table(pg_conn, embedding_table)
        
        # 4. process each csv files
        csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
        logger.info(f"CSV files found: {csv_files}")
        
        for csv_file in csv_files:
            table_name = os.path.splitext(csv_file)[0].lower()
            csv_path = os.path.join(DATA_DIR, csv_file)
            logger.info(f"Processing CSV file '{csv_file}' into table '{table_name}'")
            
            primary_key = None

            # manually assign primary kets to the tables
            if table_name == "flights":
                primary_key = "unique_id"
                logger.info("Table 'flights' primary key set to auto-generated 'unique_id'.")
            elif table_name in ["airlines", "airports"]:
                primary_key = "iata_code"
                logger.info(f"Table '{table_name}' primary key set to 'iata_code'.")
            
            import_csv_to_pg(pg_conn, table_name, csv_path, primary_key)
            
            if table_name in ["airlines", "airports"]:
                logger.info(f"Generating embeddings for table '{table_name}'")
                row_data = []
                with open(csv_path, "r", encoding="utf-8") as file:
                    csv_reader = csv.reader(file)
                    header = next(csv_reader)
                    for row in csv_reader:
                        json_string = json.dumps(row)
                        embedding = embed_model.encode(json_string)
                        row_data.append((table_name, json_string, embedding.tolist()))
                
                batch_size = 500
                logger.info(f"Inserting {len(row_data)} embeddings from '{table_name}' in batches of {batch_size}")
                
                for i in range(0, len(row_data), batch_size):
                    batch = row_data[i:i+batch_size]
                    sql = "INSERT INTO text_embeddings (table_name, row_data, embedding) VALUES (%s, %s, %s)"
                    try:
                        cursor = pg_conn.cursor()
                        execute_batch(cursor, sql, batch)
                        pg_conn.commit()
                        logger.info(f"Inserted batch of {len(batch)} embeddings")
                    except Exception as e:
                        pg_conn.rollback()
                        logger.error(f"Error inserting embeddings batch: {e}")
            
            # index creation
            if table_name == "flights":
                for column in ["origin_airport", "destination_airport", "airline", 
                               "flight_number", "tail_number", "year", "month"]:
                    create_index(pg_conn, table_name, column)
            elif table_name == "airports":
                for column in ["iata_code", "city", "state", "country"]:
                    create_index(pg_conn, table_name, column)
            elif table_name == "airlines":
                create_index(pg_conn, table_name, "iata_code")
            
            logger.info(f"Finished processing CSV file '{csv_file}'")
    
    except Exception as e:
        if pg_conn:
            pg_conn.rollback()
        logger.error(f"Error initializing database: {e}")
    finally:
        if pg_conn:
            try:
                pg_conn.close()
                logger.info("PostgreSQL connection closed")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL connection: {e}")
        print("All operations completed.")


if __name__ == "__main__":
    initialize_database_import()
    print("Database initialization complete.")