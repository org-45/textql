import google.generativeai as genai
import os
import re
import logging

import sqlparse
from dotenv import load_dotenv
from src.helper.loader import load_queries, load_schema
from src.helper.prompter import construct_prompt

load_dotenv()

logger = logging.getLogger(__name__)


def call_llm_api(prompt: str) -> str:
    """Calls the Gemini API and returns the generated text."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY not found in environment variables")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash-001')
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
    gemini_output = gemini_output.strip()

    return gemini_output

def format_llm_output_sql(sql_query: str) -> str:
    """Formats the SQL query using sqlparse."""
    try:
        formatted_sql = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
        return formatted_sql
    except Exception as e:
        logger.warning(f"Error formatting SQL: {e}")
        return sql_query


def validate_llm_output_sql(sql_query: str) -> bool:
    """
    Validates the SQL query to ensure it does not contain any potentially dangerous statements.
    """
    dangerous_patterns = [r'\bDROP\b', r'\bDELETE\b', r'\bTRUNCATE\b', r'\bALTER\b', r'\bUPDATE\b']
    for pattern in dangerous_patterns:
        if re.search(pattern, sql_query, re.IGNORECASE):
            logger.warning("Dangerous SQL keyword found! Preventing execution.")

             # raise exception
            raise ValueError("The SQL query contains a potentially dangerous statement and cannot be executed.")
        
    # it is valid if no exception was raised.
    return True

# main language ->  SQL pipeline
async def generate_sql_from_llm(natural_language_input: str) -> dict:
    """Generates a SQL query from natural language input using the Gemini API."""
    try:
        # 1. Load the queries and schema
        queries = await load_queries()
        schema = await load_schema()

        # 2. Construct the prompt
        prompt = construct_prompt(natural_language_input, queries, schema)

        # 3. Call the Gemini API
        gemini_output = call_llm_api(prompt)
        logger.debug(f"Generated SQL query: {gemini_output}")

        # 4. Clean the output
        cleaned_output = clean_llm_output(gemini_output)
        logger.debug(f"Cleaned SQL query: {cleaned_output}")

        # 5. validate sql donot contain harmful queries like drop, delete, add, update
        validate_llm_output_sql(cleaned_output)

        # 6. Format SQL query
        formatted_sql = format_llm_output_sql(cleaned_output)

        # include the original prompt in output too
        return {"data": formatted_sql, "prompt": prompt}

    except Exception as e:
        logger.exception(f"Error generating SQL query: {e}")
        return {"error": str(e), "prompt": ""}