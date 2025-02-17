import google.generativeai as genai
import os
import re
import logging
import json
import random
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

def load_queries(filepath="data/queries.json"):
    """Loads a limited number of example queries from a JSON file."""
    try:
        with open(filepath, 'r') as f:
            queries = json.load(f)
            return random.sample(queries, min(5, len(queries)))  # Limit to 5 queries
    except Exception as e:
        logger.error(f"Error loading queries from {filepath}: {e}")
        return []

def load_schema(filepath="data/schema.json"):
    """Loads the database schema from a JSON file."""
    try:
        with open(filepath, "r") as file:
            schema = json.load(file)
    except FileNotFoundError:
        schema = {
            "tables": {
                "airlines": ["IATA_CODE", "AIRLINE"],
                "airports": ["IATA_CODE", "AIRPORT", "CITY", "STATE", "COUNTRY", "LATITUDE", "LONGITUDE"],
                "flights": [
                    "YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "AIRLINE", "FLIGHT_NUMBER", "TAIL_NUMBER",
                    "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "SCHEDULED_DEPARTURE", "DEPARTURE_TIME",
                    "DEPARTURE_DELAY", "TAXI_OUT", "WHEELS_OFF", "SCHEDULED_TIME", "ELAPSED_TIME",
                    "AIR_TIME", "DISTANCE", "WHEELS_ON", "TAXI_IN", "SCHEDULED_ARRIVAL", "ARRIVAL_TIME",
                    "ARRIVAL_DELAY", "DIVERTED", "CANCELLED", "CANCELLATION_REASON", "AIR_SYSTEM_DELAY",
                    "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY"
                ]
            }
        }
    return schema
def construct_prompt(natural_language_input: str, queries: list, schema: dict) -> str:
    """Constructs the prompt for the LLM using the schema, example queries, and user input."""
    reference_prompts = "\n".join([f"- {query['description']}: {query['sql']}" for query in queries])
    table_info = "\n".join([f"Table: {table}, Columns: {', '.join(columns)}" for table, columns in schema["tables"].items()])

    prompt = f"""
    Act as a database analyst and translate the following natural language input into a SQL query:
    "{natural_language_input}"
    Use the following references for guidance. Here are the natural language and its corresponsing sql queries.
    {reference_prompts}
    Here is the schema information. These are the table columns. Understand if a given natural language has a typo as per the given schema info too.
    {table_info}
    Return the SQL query only. No other text.
    """
    return prompt


def call_gemini_api(prompt: str) -> str:
    """Calls the Gemini API and returns the generated text."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY not found in environment variables")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash-001')
    response = model.generate_content(prompt)
    return response.text


def clean_gemini_output(gemini_output: str) -> str:
    """Cleans the Gemini output by removing surrounding text."""
    gemini_output = re.sub(r'```json\s*', '', gemini_output)
    gemini_output = re.sub(r'```\s*', '', gemini_output)
    return gemini_output


def generate_sql(natural_language_input: str) -> dict:
    """Generates a SQL query from natural language input using the Gemini API."""
    try:
        # 1. Load the queries and schema
        queries = load_queries()
        schema = load_schema()

        # 2. Construct the prompt
        prompt = construct_prompt(natural_language_input, queries, schema)

        # 3. Call the Gemini API
        gemini_output = call_gemini_api(prompt)
        logger.debug(f"Generated SQL query: {gemini_output}")

        # 4. Clean the output
        cleaned_output = clean_gemini_output(gemini_output)
        logger.debug(f"Cleaned SQL query: {cleaned_output}")

        # include the original prompt in output too
        return {"data": cleaned_output, "prompt": prompt}

    except Exception as e:
        logger.exception(f"Error generating SQL query: {e}")
        return {"error": str(e), "prompt": ""}