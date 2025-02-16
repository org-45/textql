from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import re

from src.gemini_api import generate_data

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

class Item(BaseModel):
    id: int
    name: str
    value: int

def is_valid_input(user_input):
    # Check for potentially harmful characters/patterns
    pattern = re.compile(r"^[a-zA-Z0-9\s.,?!-]+$")  # Allow letters, numbers, spaces, and some punctuation
    return bool(pattern.match(user_input))

def render_notification(request: Request, message: str, type: str = "error"):
    return templates.TemplateResponse(
        "notification.html", {"request": request, "message": message, "type": type}
    )

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/hello", response_class=HTMLResponse)
async def hello_world(request: Request):
    return templates.TemplateResponse("hello.html", {"request": request})

@app.post("/generate", response_class=HTMLResponse)
async def generate_data_endpoint(request: Request, user_input: str = Form(...)):

    if not is_valid_input(user_input):
        return render_notification(request, "Invalid input. Please use only alphanumeric characters, spaces, and basic punctuation.","error")

    result = generate_data(user_input)

    data = result.get("data", [])
    prompt = result.get("prompt", "")

    return templates.TemplateResponse(
        "table.html", {"request": request, "data": data, "prompt": prompt}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True) # reload for dev