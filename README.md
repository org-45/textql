docker-compose up --build -d

pg will be running

then run the importer code 

python helper/importer.py

this will populate tables and vector embeddings 

populate env variable with necessary values

then

` uvicorn main:app --reload `
