from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List, Any
from pydantic import BaseModel, constr
from datetime import datetime
import logging

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.responses import PlainTextResponse

from src.database import DatabaseManager
from src.llm import generate_sql_from_llm

from src.helper.loader import load_queries

logger = logging.getLogger(__name__)

class QueryInput(BaseModel):
    query: constr(min_length=1, max_length=1000)  # type: ignore

class QueryResult(BaseModel):
    column_names: List[str]
    results: List[List[Any]]

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
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "queries": await load_queries(),
                    "app_name": 'textql',
                    "now": datetime.now()
                }
            )
        except Exception as e:
            logger.error(f"Error in read_root: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    @app.post("/generate-sql", response_class=HTMLResponse)
    @limiter.limit("1/15seconds")
    async def generate_sql_endpoint(
        request: Request,
        natural_language_input: str = Form(...),
        db: DatabaseManager = Depends(get_db)
    ):
        try:
            response = await generate_sql_from_llm(natural_language_input)
            sql_query = response["data"].replace('sql', '').strip()
            column_names, results = await db.execute_query(sql_query)
            
            return templates.TemplateResponse(
                "table.html",
                {
                    "request": request,
                    "column_names": column_names,
                    "results": results,
                    "sql_query": sql_query
                }
            )
        except Exception as e:
            logger.error(f"Error in generate_sql_endpoint: {e}")
            return templates.TemplateResponse(
                "notification.html",
                {
                    "request": request,
                    "message": f"Error generating SQL query: {e}",
                    "type": "error"
                }
            )

    @app.post(f"{api_prefix}/query", response_model=QueryResult)
    async def execute_query_api(
        query_input: QueryInput,
        db: DatabaseManager = Depends(get_db)
    ):
        try:
            column_names, results = await db.execute_query(query_input.query)
            return QueryResult(column_names=column_names, results=results)
        except Exception as e:
            logger.error(f"API error in execute_query: {e}")
            raise HTTPException(status_code=500, detail=str(e))