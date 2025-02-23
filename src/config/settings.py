
from dotenv import load_dotenv
import os
import yaml
from typing import List
import logging

logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv()

# Load YAML configuration from textql.yaml
def load_yaml_config(file_path: str = "textql.yaml") -> dict:
    try:
        with open(file_path, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.warning(f"{file_path} not found, using defaults")
        return {}
    except Exception as e:
        logger.error(f"Error loading {file_path}: {str(e)}")
        raise

config = load_yaml_config()

APP_NAME: str = os.getenv("APP_NAME", config.get("app_name", "TextQL"))
ALLOWED_HOSTS: List[str] = os.getenv("ALLOWED_HOSTS", ",".join(config.get("allowed_hosts", ["*"]))).split(",")
ALLOWED_ORIGINS: List[str] = os.getenv("ALLOWED_ORIGINS", ",".join(config.get("allowed_origins", ["*"]))).split(",")
API_PREFIX: str = os.getenv("API_PREFIX", config.get("api_prefix", "/api/v1"))
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", config.get("postgres_host", "localhost"))
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", config.get("postgres_port", 5432)))
SENTENCE_TRANSFORMER_MODEL: str = os.getenv("SENTENCE_TRANSFORMER_MODEL", config.get("sentence_transformer_model", "all-MiniLM-L6-v2"))
LLM: str = os.getenv("LLM", config.get("llm", "gemini-2.0-flash-001"))
SQL_EXECUTION_TIMEOUT: int = config.get("sql_execution_timeout", 10)
VECTOR_ROWS_IN_PROMPT:int = config.get("vector_rows_in_prompt",2)

# Secrets from .env only
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "vector_db")
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY")