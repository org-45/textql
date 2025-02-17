first run the importer code 

python helper/importer.py

this will create textql.db at the root of the project based on the data from CSV

populate env variable with necessary values

then

` uvicorn main:app --reload `
