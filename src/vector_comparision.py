import psycopg2
import logging
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DB = "vector_db"

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

def get_similar_rows(query_embedding, pg_conn, top_k=5):
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
        cursor.execute(sql, (top_k,))
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

async def get_similar_rows_from_pg(user_query: str) -> str:
    logger.info("Starting call to PostgreSQL")
    pg_conn = connect_to_pg()
    if pg_conn is None:
        return ""
    
    embed_model = SentenceTransformer('all-MiniLM-L6-v2')
    logger.info("Embedding model loaded")

    query_embedding = embed_model.encode(user_query)
    logger.info("Query embedding created")

    results = get_similar_rows(query_embedding, pg_conn, top_k=1)
    logger.info("Similar rows retrieved")

    table_of_content = format_results(results)
    pg_conn.close()
    return {table_of_content, user_query}