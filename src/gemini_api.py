import google.generativeai as genai
import json
import os
from dotenv import load_dotenv
import re

load_dotenv()

def generate_data(user_input: str):
  api_key = os.environ.get("GEMINI_API_KEY")

  if not api_key:
      return {"error": "GEMINI_API_KEY not found in environment variables", "prompt":""}
  try:
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-pro')

    prompt = f"Generate data related to {user_input} in a JSON format array of objects. Each object should have 'id' (integer), 'name' (string), and 'value' (integer) keys. Return exactly 3 objects. ONLY return valid JSON. No other text. "
    
    response = model.generate_content(prompt)
    gemini_output = response.text

    #remove surrounding text
    gemini_output = re.sub(r'```json\s*', '', gemini_output)
    gemini_output = re.sub(r'```\s*', '', gemini_output)

    try:
        data = json.loads(gemini_output)
        return {"data": data, "prompt": prompt}
    except json.JSONDecodeError as e:
        print(f"Error: Gemini returned invalid JSON: {gemini_output}")
        print(f"JSONDecodeError: {e}")
        return {"data": [], "prompt": prompt}

  except Exception as e:
        print(f"Error: Could not connect to Gemini API: {e}")
        return {"error": str(e), "prompt": prompt}
