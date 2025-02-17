import google.generativeai as genai
import json
import os
from dotenv import load_dotenv
import re
import logging
import random

logger = logging.getLogger(__name__)

load_dotenv()

# only load 5 items from json, prompt gets smaller
def load_queries():
    with open('data/queries.json') as f:
        queries = json.load(f)
        return random.sample(queries, min(5, len(queries)))

def load_schema():
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
    
def generate_sql( natural_language_input: str):
  api_key = os.environ.get("GEMINI_API_KEY")

  if not api_key:
      return {"error": "GEMINI_API_KEY not found in environment variables", "prompt":""}
  try:
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash-001')
    queries = load_queries()
    schema = load_schema()

    reference_prompts = "\n".join([f"- {query['description']}: {query['sql']}" for query in queries])
    table_info = "\n".join([f"Table: {table}, Columns: {', '.join(columns)}" for table, columns in schema["tables"].items()])

    prompt = f"""
    Act as a database analyst and translate the following natural language input into a SQL query:
    "{natural_language_input}"
    Use the following references for guidance:
    {reference_prompts}
    Here is the schema information:
    {table_info}
    Return the SQL query only. No other text.
    """
    
    response = model.generate_content(prompt)
    gemini_output = response.text
    logger.debug(f"Generated SQL query: {gemini_output}")

    #remove surrounding text
    gemini_output = re.sub(r'```json\s*', '', gemini_output)
    gemini_output = re.sub(r'```\s*', '', gemini_output)

    logger.debug(f"Generated SQL query: {gemini_output}")

    return {"data": gemini_output, "prompt": prompt}

  except Exception as e:
        print(f"Error: Could not connect to Gemini API: {e}")
        return {"error": str(e), "prompt": prompt}