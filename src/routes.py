import re
import logging
import uuid
from fastapi import FastAPI, Request, Form, HTTPException, Depends, Query
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
import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Keyword

logger = logging.getLogger(__name__)

class QueryInput(BaseModel):
    query: constr(min_length=1, max_length=1000) #type: ignore

class QueryResult(BaseModel):
    column_names: List[str]
    results: List[List[Any]]

def postprocess_llm_pipeline_data(response: object) -> str:
    return response["data"].replace('\n', ' ').strip()

def validate_sql_before_execute(sql_query: str) -> bool:
    """Validates the SQL query to ensure it does not contain any potentially dangerous statements."""
    dangerous_keywords = {"DROP", "DELETE", "TRUNCATE", "ALTER", "UPDATE", "CREATE", "GRANT", "REVOKE"}
    
    try:
        parsed = sqlparse.parse(sql_query)
        for statement in parsed:
            for token in statement.tokens:
                if token.ttype == Keyword and token.value.upper() in dangerous_keywords:
                    logger.warning(f"Dangerous SQL keyword '{token.value}' found! Preventing execution.")
                    raise ValueError(f"The SQL query contains a potentially dangerous statement: '{token.value}'")
    except Exception as e:
        logger.error(f"Error during SQL validation: {e}")
        raise ValueError("Invalid SQL query.")
    
    return True

def sanitize_query(input_text: str) -> str:
    """Sanitize user query: allow only alphabet and numbers, limit to 50 words."""
    sanitized = re.sub(r'[^a-zA-Z0-9\s]', '', input_text)
    words = sanitized.split()
    limited_words = words[:50]
    return ' '.join(limited_words)

def setup_routes(app: FastAPI, templates: Jinja2Templates, api_prefix: str):
    limiter = Limiter(key_func=get_remote_address)
    app.state.limiter = limiter

    async def get_db(request: Request) -> DatabaseManager:
        return request.app.state.db

    @app.exception_handler(RateLimitExceeded)
    async def custom_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
        return PlainTextResponse(str(exc), status_code=429)

    @app.get("/", response_class=HTMLResponse)
    async def read_root(request: Request):
        try:
            template_response = templates.TemplateResponse(
                "index.html",
                {"request": request, "app_name": "TextQL"}
            )
            logger.info("Index page is loaded.")
            return template_response
        except Exception as e:
            logger.error(f"Error in read_root: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    @app.post("/generate-sql", response_class=HTMLResponse)
    @limiter.limit("1/15seconds")
    async def generate_sql_endpoint(request: Request, natural_language_input: str = Form(...), db: DatabaseManager = Depends(get_db)):
        try:
            sanitized_input = sanitize_query(natural_language_input)
            if not sanitized_input.strip():
                raise ValueError("Sanitized query is empty or invalid")

            pipeline_response = await generate_sql_from_llm(db, sanitized_input)
            if "error" in pipeline_response:
                return templates.TemplateResponse(
                    "text-to-sql.html",
                    {"request": request, "message": pipeline_response["error"], "type": "error"}
                )
            sql_query = postprocess_llm_pipeline_data(pipeline_response)

            query_token = str(uuid.uuid4())
            app.state.sql_store[query_token] = {"nl": sanitized_input, "sql": sql_query}
            logger.info("SQL query generated and stored with token %s", query_token)

            return templates.TemplateResponse(
                "text-to-sql.html",
                {"request": request, "sql_query": sql_query, "query_token": query_token}
            )
        except Exception as e:
            logger.error(f"Error in generate_sql_endpoint: {e}")
            return templates.TemplateResponse(
                "text-to-sql.html",
                {"request": request, "message": f"Error generating SQL query: {e}", "type": "error"}
            )

    @app.post("/execute-sql", response_class=HTMLResponse)
    @limiter.limit("1/15seconds")
    async def execute_sql_endpoint(request: Request, query_token: str = Form(...), db: DatabaseManager = Depends(get_db)):
        try:
            query_data = app.state.sql_store.get(query_token)
            if not query_data:
                raise HTTPException(status_code=400, detail="Invalid or expired query token.")
            
            sql_query = query_data["sql"]
            
            validate_sql_before_execute(sql_query)

            column_names, results = await db.execute_query(sql_query)
            logger.info("SQL query executed successfully for token %s", query_token)

            del app.state.sql_store[query_token]

            return templates.TemplateResponse(
                "text-to-sql.html",
                {"request": request, "sql_query": sql_query, "query_token": None, "column_names": column_names, "results": results}
            )
        
        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Error in execute_sql_endpoint: {e}")
            return templates.TemplateResponse(
                "text-to-sql.html",
                {"request": request, "message": f"Error executing SQL query: {e}", "type": "error"}
            )

    @app.post("/submit-feedback", response_class=HTMLResponse)
    @limiter.limit("1/5seconds")
    async def submit_feedback(request: Request, query_token: str = Form(...), feedback: str = Form(...), corrected_sql: str = Form(default=None), db: DatabaseManager = Depends(get_db)):
        try:
            query_data = app.state.sql_store.get(query_token)
            if not query_data:
                raise HTTPException(status_code=400, detail="Invalid or expired query token.")

            natural_language_input = query_data["nl"]
            original_sql = query_data["sql"]

            if feedback == "yes":
                await db.store_feedback(natural_language_input, original_sql, "yes")
                return templates.TemplateResponse(
                    "feedback_response.html",
                    {
                        "request": request,
                        "message": "Thank you for your feedback!",
                        "query_token": query_token,
                        "show_back": True
                    }
                )
            elif feedback == "no" and not corrected_sql:
                return templates.TemplateResponse(
                    "feedback_correction.html",
                    {
                        "request": request,
                        "query_token": query_token,
                        "original_sql": original_sql
                    }
                )
            elif feedback == "no" and corrected_sql:
                await db.store_feedback(natural_language_input, original_sql, "no", corrected_sql)
                return templates.TemplateResponse(
                    "feedback_response.html",
                    {
                        "request": request,
                        "message": "Correction submitted. Thank you!",
                        "corrected_sql": corrected_sql,
                        "query_token": query_token,
                        "show_back": True
                    }
                )
            else:
                raise ValueError("Invalid feedback option")
        except Exception as e:
            logger.error(f"Error in submit_feedback: {e}")
            return templates.TemplateResponse(
                "feedback_response.html",
                {
                    "request": request,
                    "message": f"Error submitting feedback: {str(e)}",
                    "type": "error"
                }
            )

    @app.post("/get-similar-rows", response_class=HTMLResponse)
    async def get_similar_rows_endpoint(
        request: Request,
        user_query: str = Form(...),
        page: int = Query(1, ge=1),
        page_size: int = Query(10, ge=1, le=100),
        db: DatabaseManager = Depends(get_db)
    ):
        """Endpoint to fetch similar rows with pagination."""
        try:
            formatted_rows, _ = await get_similar_rows_from_vector(db, user_query, VECTOR_ROWS_IN_PROMPT, page, page_size)
            return templates.TemplateResponse(
                "text-to-sql.html",
                {"request": request, "similar_rows": formatted_rows, "page": page, "page_size": page_size}
            )
        except Exception as e:
            logger.error(f"Error in get_similar_rows_endpoint: {e}")
            return templates.TemplateResponse(
                "text-to-sql.html",
                {"request": request, "message": f"Error fetching similar rows: {e}", "type": "error"}
            )