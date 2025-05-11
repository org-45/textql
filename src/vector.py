import logging
from src.database import DatabaseManager
from sentence_transformers import SentenceTransformer
from src.config.settings import SENTENCE_TRANSFORMER_MODEL,VECTOR_ROWS_IN_PROMPT

logger = logging.getLogger(__name__)

async def get_similar_rows_from_vector(db: DatabaseManager, user_query: str, num_of_rows: int = VECTOR_ROWS_IN_PROMPT) -> tuple:
    """Fetch similar rows using vector embeddings synchronously."""
    try:
        embed_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL)
        query_embedding = embed_model.encode(user_query)
        logger.info("Query embedding created")
        embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'
        results = await db.get_similar_rows(embedding_str, num_of_rows)
        logger.info("Similar rows retrieved")
        
        formatted_rows = "".join([f"Table: {row[0]}, Data: {row[1]}\n" for row in results])
        return formatted_rows, user_query
    except ValueError as ve:
        logger.error(f"Value error in vector search: {ve}")
        return "", user_query
    except Exception as e:
        logger.error(f"Unexpected error in vector search: {e}")
        return "", user_query