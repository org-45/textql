import logging
import uuid
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List, Any
from pydantic import BaseModel, constr
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.responses import PlainTextResponse
from src.llm import generate_sql_from_llm

logger = logging.getLogger(__name__)

class QueryInput(BaseModel):
    query: constr(min_length=1, max_length=1000)

class QueryResult(BaseModel):
    column_names: List[str]
    results: List[List[Any]]

def postprocess_llm_pipeline_data(response: object) -> str:
    return response["data"].replace('\n', ' ').strip()

def setup_routes(app: FastAPI, templates: Jinja2Templates, api_prefix: str):
    limiter = Limiter(key_func=get_remote_address)
    app.state.limiter = limiter

    @app.exception_handler(RateLimitExceeded)
    def custom_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
        return PlainTextResponse(str(exc), status_code=429)

    @app.get("/", response_class=HTMLResponse)
    def read_root(request: Request):
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
    def generate_sql_endpoint(request: Request, natural_language_input: str = Form(...)):
        try:
            db = request.app.state.db
            pipeline_response = generate_sql_from_llm(db, natural_language_input)
            if "error" in pipeline_response:
                return templates.TemplateResponse(
                    "text-to-sql.html",
                    {"request": request, "message": pipeline_response["error"], "type": "error"}
                )
            sql_query = postprocess_llm_pipeline_data(pipeline_response)

            query_token = str(uuid.uuid4())
            app.state.sql_store[query_token] = {"nl": natural_language_input, "sql": sql_query}
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
    def execute_sql_endpoint(request: Request, query_token: str = Form(...)):
        try:
            query_data = app.state.sql_store.get(query_token)
            if not query_data:
                raise HTTPException(status_code=400, detail="Invalid or expired query token.")
            
            sql_query = query_data["sql"]
            db = request.app.state.db
            column_names, results = db.execute_query(sql_query)
            logger.info("SQL query executed successfully for token %s", query_token)

            del app.state.sql_store[query_token]

            return templates.TemplateResponse(
                "text-to-sql.html",
                {"request": request, "sql_query": sql_query, "query_token": None, "column_names": column_names, "results": results}
            )
        except Exception as e:
            logger.error(f"Error in execute_sql_endpoint: {e}")
            return templates.TemplateResponse(
                "text-to-sql.html",
                {"request": request, "message": f"Error executing SQL query: {e}", "type": "error"}
            )

    @app.post("/submit-feedback", response_class=HTMLResponse)
    @limiter.limit("1/5seconds")
    def submit_feedback(request: Request, query_token: str = Form(...), feedback: str = Form(...), corrected_sql: str = Form(default=None)):
        try:
            query_data = app.state.sql_store.get(query_token)
            if not query_data:
                raise HTTPException(status_code=400, detail="Invalid or expired query token.")

            natural_language_input = query_data["nl"]
            original_sql = query_data["sql"]
            db = request.app.state.db

            if feedback == "yes":
                db.store_feedback(natural_language_input, original_sql, "yes")
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
                db.store_feedback(natural_language_input, original_sql, "no", corrected_sql)
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