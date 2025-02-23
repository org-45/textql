import google.generativeai as genai
import re
import logging
import sqlparse

from src.database import DatabaseManager
from dotenv import load_dotenv
from src.helper.loader import load_queries, load_schema_and_samples
from src.helper.prompter import construct_prompt
from src.vector_comparision import get_similar_rows_from_vector
from src.config.settings import GEMINI_API_KEY,LLM

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def call_llm_api(prompt: str) -> str:
    """Calls the Gemini API and returns the generated text."""
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(LLM)
    response = model.generate_content(prompt)
    return response.text

def clean_llm_output(gemini_output: str) -> str:
    """Cleans the Gemini output by removing surrounding text."""
    # ignore case
    gemini_output = re.sub(r'```sql\s*', '', gemini_output, flags=re.IGNORECASE)
    gemini_output = re.sub(r'```\s*', '', gemini_output, flags=re.IGNORECASE)

    # remove leading "SQL"
    gemini_output = re.sub(r'^SQL\s*', '', gemini_output, flags=re.IGNORECASE)  

    # remove trailing semicolon
    gemini_output = re.sub(r';\s*$', '', gemini_output)

    # remove any leading/trailing whitespace
    return gemini_output.strip()

def format_llm_output_sql(sql_query: str) -> str:
    """Formats the SQL query using sqlparse."""
    try:
        formatted_sql = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
        return formatted_sql
    except Exception as e:
        logger.warning(f"Error formatting SQL: {e}")
        return sql_query
# main language->SQL pipeline
async def generate_sql_from_llm(db: DatabaseManager, natural_language_input: str) -> dict:
    """Generates a SQL query from natural language input using the Gemini API."""
    try:
        # 1. Load the queries and schema
        queries = await load_queries()
        schema = await load_schema_and_samples(db)
        # 2. Get similar rows from vector table
        similar_rows, _ = await get_similar_rows_from_vector(db, natural_language_input, 2)
        # 3. Construct the prompt
        prompt = construct_prompt(natural_language_input, similar_rows, queries, schema)
            
        # 4. Call the LLM API
        gemini_output = await call_llm_api(prompt)
        logger.debug(f"Generated SQL query: {gemini_output}")

        # 5. Clean the output
        cleaned_output = clean_llm_output(gemini_output)
        logger.debug(f"Cleaned SQL query: {cleaned_output}")

        # 6. Format SQL query
        formatted_sql = format_llm_output_sql(cleaned_output)

        # include the original prompt in output too
        return {"data": formatted_sql, "prompt": prompt}

    except Exception as e:
        logger.exception(f"Error generating SQL query: {e}")
        return {"error": str(e), "prompt": ""}