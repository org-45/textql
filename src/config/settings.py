from dotenv import load_dotenv
import os

load_dotenv()

APP_NAME = os.getenv("APP_NAME", "TextQL")
ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "*").split(",")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
API_PREFIX = os.getenv("API_PREFIX", "/api/v1")

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "vector_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

SENTENCE_TRANSFORMER_MODEL =os.getenv("SENTENCE_TRANSFORMER_MODEL","all-MiniLM-L6-v2") 
LLM = os.getenv('LLM','gemini-2.0-flash-001')