import logging
import uuid

from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List, Any
from pydantic import BaseModel, constr

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.responses import PlainTextResponse

from src.database import DatabaseManager
from src.llm import generate_sql_from_llm

logger = logging.getLogger(__name__)

class QueryInput(BaseModel):
    query: constr(min_length=1, max_length=1000)  # type: ignore

class QueryResult(BaseModel):
    column_names: List[str]
    results: List[List[Any]]

def postprocess_llm_pipeline_data(response:object)->str:
    return response["data"].replace('\n', ' ').strip()

def setup_routes(app: FastAPI, templates: Jinja2Templates, api_prefix: str):

    # initialize the limiter
    limiter = Limiter(key_func=get_remote_address)
    app.state.limiter = limiter

    # exception Handler
    @app.exception_handler(RateLimitExceeded)
    async def custom_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
        return PlainTextResponse(str(exc), status_code=429)
    async def get_db(request: Request) -> DatabaseManager:
        return request.app.state.db

    @app.get("/", response_class=HTMLResponse)
    async def read_root(request: Request, db: DatabaseManager = Depends(get_db)):
        try:
            template_response = templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "app_name": "TextQL",
                }
            )
            logger.info ("Index page is loaded.")
            return template_response
        except Exception as e:
            logger.error(f"Error in generate_sql_endpoint: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    @app.post("/generate-sql", response_class=HTMLResponse)
    @limiter.limit("1/15seconds")
    async def generate_sql_endpoint(
        request: Request,
        natural_language_input: str = Form(...),
    ):
        try:
            pipeline_response = await generate_sql_from_llm(natural_language_input)
            sql_query = postprocess_llm_pipeline_data(pipeline_response)

            query_token = str(uuid.uuid4())
            request.app.state.sql_store[query_token] = sql_query
            logger.info("SQL query generated and stored with token %s", query_token)

            template_response = templates.TemplateResponse(
                "text-to-sql.html",
                {
                    "request": request,
                    "sql_query": sql_query,
                    "query_token": query_token,
                }
            )
            logger.info("query is generated")
            return template_response
        except Exception as e:
            logger.error(f"Error in generate_sql_endpoint: {e}")
            template_response =  templates.TemplateResponse(
                "text-to-sql.html",
                {
                    "request": request,
                    "message": f"Error generating SQL query: {e}",
                    "type": "error",
                }
            )
            return template_response
        

    @app.post("/execute-sql", response_class=HTMLResponse)
    @limiter.limit("1/15seconds")
    async def execute_sql_endpoint(
        request: Request,
        query_token: str = Form(...),
        db: DatabaseManager = Depends(get_db)
    ):
        try:
            sql_query = request.app.state.sql_store.get(query_token)
            if not sql_query:
                raise HTTPException(status_code=400, detail="Invalid or expired query token.")

            column_names, results = await db.execute_query(sql_query)
            logger.info("SQL query executed successfully for token")

            del request.app.state.sql_store[query_token]

            template_response = templates.TemplateResponse(
                "text-to-sql.html",
                {
                    "request": request,
                    "sql_query": sql_query,
                    "query_token": None,
                    "column_names": column_names,
                    "results": results,
                }
            )
            logger.info("query is executed")
            return template_response
        except Exception as e:
            logger.error(f"Error in execute_sql_endpoint: {e}")
            template_response =  templates.TemplateResponse(
                "text-to-sql.html",
                {
                    "request": request,
                    "message": f"Error executing SQL query: {e}",
                    "type": "error",
                }
            )
            return template_response