import logging
from src.database import DatabaseManager
from sentence_transformers import SentenceTransformer
from src.config.settings import SENTENCE_TRANSFORMER_MODEL,VECTOR_ROWS_IN_PROMPT

logger = logging.getLogger(__name__)

async def get_similar_rows_from_vector(db: DatabaseManager, user_query: str, num_of_rows: int = VECTOR_ROWS_IN_PROMPT) -> tuple:
    """Fetch similar rows using vector embeddings synchronously."""
    try:
        # Load the embedding model
        try:
            embed_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL)
            logger.info("SentenceTransformer model loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading SentenceTransformer model: {e}")
            raise RuntimeError("Failed to load the embedding model. Please check the model configuration.")

        # Generate query embedding
        try:
            query_embedding = embed_model.encode(user_query)
            logger.info("Query embedding created successfully.")
        except Exception as e:
            logger.error(f"Error generating query embedding: {e}")
            raise ValueError("Failed to generate query embedding. Please ensure the input query is valid.")

        # Convert embedding to string format
        embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'

        # Fetch similar rows from the database
        try:
            results = await db.get_similar_rows(embedding_str, num_of_rows)
            logger.info("Similar rows retrieved successfully.")
        except Exception as e:
            logger.error(f"Error fetching similar rows from the database: {e}")
            raise RuntimeError("Failed to retrieve similar rows. Please check the database connection or query.")

        # Format the results
        formatted_rows = "".join([f"Table: {row[0]}, Data: {row[1]}\n" for row in results])
        return formatted_rows, user_query

    except Exception as e:
        logger.error(f"Error in vector search: {e}")
        return f"An error occurred during vector search: {e}", user_query