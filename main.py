from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.gemini_api import generate_data

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

class Item(BaseModel):
    id: int
    name: str
    value: int
    
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/hello", response_class=HTMLResponse)
async def hello_world(request: Request):
    return templates.TemplateResponse("hello.html", {"request": request})

@app.post("/generate", response_class=HTMLResponse)
async def generate_data_endpoint(request: Request, user_input: str = Form(...)):
    result = generate_data(user_input)

    data = result.get("data", [])
    prompt = result.get("prompt", "")

    return templates.TemplateResponse(
        "table.html", {"request": request, "data": data, "prompt": prompt}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True) # reload for dev