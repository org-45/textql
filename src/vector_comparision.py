import psycopg2
import logging
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DB = "vector_db"
SENTENCE_TRANSFORMER_MODEL="all-MiniLM-L6-v2"

def connect_to_pg():
    try:
        logger.info("Attempting to connect to PostgreSQL...")
        pg_conn = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB,
            host="localhost",
            port="5432"
        )
        logger.info("Successfully connected to PostgreSQL")
        return pg_conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return None

def get_similar_rows(query_embedding, pg_conn, num_of_row=5):
    try:
        cursor = pg_conn.cursor()
        logger.info("Cursor created")
        sql = f"""
            SELECT table_name, row_data, embedding <#> '{query_embedding.tolist()}' AS similarity
            FROM text_embeddings
            ORDER BY similarity ASC
            LIMIT %s
        """
        logger.debug(f"Executing SQL query: {sql}")
        cursor.execute(sql, (num_of_row,))
        logger.info("Query executed")

        results = cursor.fetchall()
        logger.info("Results fetched")

        cursor.close()
        logger.info("Cursor closed")
        return results
    except Exception as e:
        logger.error(f"Error retrieving similar rows: {e}")
        return []

def format_results(results):
    try:
        formatted_rows = ""
        for row in results:
            table_name, row_data, _ = row
            formatted_rows += f"Table: {table_name}, Data: {row_data}\n"
        return formatted_rows
    except Exception as e:
        logger.exception("Error formatting results")
        return ""

async def get_similar_rows_from_vector(user_query: str, num_of_row: int) -> tuple:
    
    logger.info("Starting call to PostgreSQL")
    pg_conn = connect_to_pg()
    
    if pg_conn is None:
        return ""
    
    try:
        embed_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL)
        logger.info("Embedding model loaded")

        query_embedding = embed_model.encode(user_query)
        logger.info("Query embedding created")

        results = get_similar_rows(query_embedding, pg_conn, num_of_row)
        logger.info("Similar rows retrieved")

        similar_rows = format_results(results)
        return (similar_rows, user_query)
    finally:
        pg_conn.close()