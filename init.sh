#!/bin/bash

# check for required commands
command -v docker >/dev/null 2>&1 || { echo "Error: docker is not installed."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "Error: python3 is not installed."; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "Error: curl is not installed."; exit 1; }
command -v unzip >/dev/null 2>&1 || { echo "Error: unzip is not installed."; exit 1; }

# load environment variables
if [ ! -f ".env" ]; then
  echo "Error: .env file not found!"
  exit 1
fi
export $(grep -v '^#' .env | xargs)

# function to check and download data
check_and_download_data() {
    local DATA_DIR="data"
    local REQUIRED_FILES=("flights.csv" "airlines.csv" "airports.csv")
    local MISSING_FILES=false

    # check for required CSV files
    for file in "${REQUIRED_FILES[@]}"; do
        if [ ! -f "$DATA_DIR/$file" ]; then
            MISSING_FILES=true
            echo "Missing required file: $file"
        fi
    done

    # if any files are missing, download dataset
    if [ "$MISSING_FILES" = true ]; then
        echo "Downloading missing files..."
        
        # Download the dataset
        echo "Downloading flight delays dataset..."
        if curl -L -o "$DATA_DIR/flight-delays.zip" \
            https://www.kaggle.com/api/v1/datasets/download/usdot/flight-delays; then
            
            echo "Download completed. Extracting files..."
            unzip -o "$DATA_DIR/flight-delays.zip" -d "$DATA_DIR/"
            rm "$DATA_DIR/flight-delays.zip"
            
            # verify files after download
            for file in "${REQUIRED_FILES[@]}"; do
                if [ ! -f "$DATA_DIR/$file" ]; then
                    echo "Error: Failed to extract $file"
                    exit 1
                fi
            done
            
            echo "Successfully downloaded and extracted all required files"
        else
            echo "Error: Failed to download dataset"
            exit 1
        fi
    else
        echo "All required CSV files present in data directory"
    fi
}

# check and download data if needed
check_and_download_data

# get container ID
CONTAINER_ID=$(docker ps -q --filter "name=textql_db")
if [ -z "$CONTAINER_ID" ]; then
  echo "PostgreSQL container not found! Please start with docker-compose up --build -d"
  exit 1
fi

# wait for PostgreSQL to become healthy
MAX_TRIES=10
WAIT_SECONDS=5
CONTAINER_HEALTHY=false

for ((i=1; i<=MAX_TRIES; i++)); do
  STATUS=$(docker inspect --format '{{.State.Health.Status}}' "$CONTAINER_ID")
  if [ "$STATUS" == "healthy" ]; then
    CONTAINER_HEALTHY=true
    break
  fi
  echo "Waiting for PostgreSQL to become healthy (attempt $i/$MAX_TRIES)..."
  sleep $WAIT_SECONDS
done

if [ "$CONTAINER_HEALTHY" != true ]; then
  echo "PostgreSQL container did not become healthy after $MAX_TRIES attempts. Exiting."
  exit 1
fi

# check if table exists using direct SQL query
TABLE_EXISTS=$(docker exec "$CONTAINER_ID" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t \
  -c "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'text_embeddings')")

# clean up output and check result
if [ "$(echo "$TABLE_EXISTS" | tr -d '[:space:]')" = "t" ]; then
  echo "Table 'text_embeddings' exists."
else
  echo "Table 'text_embeddings' does not exist. Running importer..."

  # set up virtual environment
  if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
  fi

  echo "Activating virtual environment..."
  source .venv/bin/activate

  if [ -f "requirements.txt" ]; then
    echo "Installing dependencies..."
    pip install --no-cache-dir -r requirements.txt
  fi

  python -m src.helper.importer
fi

# start FastAPI application
echo "Starting FastAPI application..."
exec uvicorn main:app --host 127.0.0.1 --port 8000 --reload