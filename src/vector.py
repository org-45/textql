import logging
import traceback  # Added for detailed error traceback
from src.database import DatabaseManager
from sentence_transformers import SentenceTransformer
from src.config.settings import SENTENCE_TRANSFORMER_MODEL,VECTOR_ROWS_IN_PROMPT

logger = logging.getLogger(__name__)

async def get_similar_rows_from_vector(db: DatabaseManager, user_query: str, num_of_rows: int = VECTOR_ROWS_IN_PROMPT, page: int = 1, page_size: int = 10) -> tuple:
    """Fetch similar rows using vector embeddings synchronously with pagination."""
    try:
        embed_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL)
        query_embedding = embed_model.encode(user_query)
        logger.info("Query embedding created")
        embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'
        results = await db.get_similar_rows(embedding_str, num_of_rows)
        logger.info("Similar rows retrieved")

        # Implement pagination
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        paginated_results = results[start_index:end_index]

        formatted_rows = "".join([f"Table: {row[0]}, Data: {row[1]}\n" for row in paginated_results])
        return formatted_rows, user_query
    except ValueError as ve:
        logger.error(f"ValueError in vector search: {ve}")
        return "Error: Invalid input for vector search.", user_query
    except Exception as e:
        logger.error(f"Unexpected error in vector search: {e}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        return "Error: Unable to retrieve similar rows due to an internal issue.", user_query