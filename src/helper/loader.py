import json
import logging
import random

logger = logging.getLogger(__name__)

async def load_queries(filepath="data/queries.json"):
    """Loads a limited number of example queries from a JSON file."""
    try:
        with open(filepath, 'r') as f:
            queries = json.load(f)
            return random.sample(queries, min(5, len(queries)))
    except Exception as e:
        logger.error(f"Error loading queries from {filepath}: {e}")
        return []
    
async def load_schema(filepath="data/schema.json"):
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