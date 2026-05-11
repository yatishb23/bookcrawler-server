from google import genai
import json
from app.config import settings

async def analyze_combined_resumes(combined_text: str) -> dict:
    """
    Sends the combined resume text to Gemini to extract a unified contact profile
    and summarize the findings.
    """
    if not settings.gemini_api_key:
        return {"error": "GEMINI_API_KEY is not configured", "summary": "Gemini API key is missing."}

    prompt = f"""
    You are an expert recruiter and data parser. I am providing you with the text extracted from multiple PDF resumes found online. 
    Your task is to analyze this text and extract:
    1. The primary Contact Info (Name, Emails, Phone numbers, LinkedIn/GitHub links).
    2. A brief 2-3 sentence summary of the person's professional profile based on all the documents.
    3. The main skills or technologies they are proficient in.

    Return the final result strictly as a valid JSON object with the following schema, and do not include any markdown backticks or formatting outside the JSON object:
    {{
        "name": "string",
        "emails": ["string"],
        "phones": ["string"],
        "links": ["string"],
        "summary": "string",
        "skills": ["string"]
    }}

    Text Extract:
    ---
    {combined_text[:30000]} # Limit text length to avoid token limits
    ---
    """
    
    try:
        client = genai.Client(api_key=settings.gemini_api_key)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
        )
        text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"[gemini_service] Error calling Gemini: {e}")
        return {"error": str(e), "summary": "Failed to analyze resumes."}
