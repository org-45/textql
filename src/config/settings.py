from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Settings
APP_NAME = os.getenv("APP_NAME", "TextQL")
ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "*").split(",")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./textql.db")
API_PREFIX = os.getenv("API_PREFIX", "/api/v1")