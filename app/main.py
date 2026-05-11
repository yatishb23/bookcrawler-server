import json
import uuid
from datetime import datetime

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from app.models import SavedBook
from app.services.crawler import search_book
from app.services.resume_crawler import search_book as search_resume
from app.services.pdf_service import fetch_pdf_bytes, first_page_preview_jpeg, extract_pdf_text_from_bytes
from app.services.redis_client import get_redis, close_redis
from app.services.gemini_service import analyze_combined_resumes
import asyncio

CACHE_EXPIRATION = 60 * 60 * 24
VISITOR_KEY = "unique_visitors_crawler"

app = FastAPI(title="PDF Crawler Backend", version="1.0.0")

origins = [
    "http://localhost:3000",
    "https://bookcrawler.vercel.app", 
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    """Initialize Redis connection on startup"""
    try:
        await get_redis()
    except Exception as e:
        print(f"⚠️  Warning: Redis unavailable on startup: {e}")
        print("Backend will continue, but caching/bookmarks will be disabled")


@app.on_event("shutdown")
async def shutdown_event():
    """Close Redis connection on shutdown"""
    await close_redis()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/getBooks")
async def get_books(q: str | None = Query(default=None)):
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Book name query parameter 'q' is required")

    book_name = q.strip()
    cache_key = f"search:{':'.join(book_name.lower().split())}"

    # Try to get from cache, but don't fail if Redis is down
    try:
        client = await get_redis()
        cached = await client.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception as exc:
        print(f"[redis] Cache get error: {exc}")

    results = await search_book(book_name)

    if not results:
        raise HTTPException(status_code=404, detail="No books found")

    # Try to cache results, but don't fail if Redis is down
    try:
        client = await get_redis()
        await client.set(cache_key, json.dumps([r.model_dump() for r in results]), ex=CACHE_EXPIRATION)
    except Exception as exc:
        print(f"[redis] Cache set error: {exc}")

    return [r.model_dump() for r in results]


@app.get("/api/v1/getResumes")
async def get_resumes(firstName: str | None = Query(default=None), lastName: str | None = Query(default=None)):
    if not firstName or not lastName:
        raise HTTPException(status_code=400, detail="Query parameters 'firstName' and 'lastName' are required")

    # cache_key = f"resume:{firstName.strip().lower()}:{lastName.strip().lower()}"

    # # Try to get from cache, but don't fail if Redis is down
    # try:
    #     client = await get_redis()
    #     cached = await client.get(cache_key)
    #     if cached:
    #         return json.loads(cached)
    # except Exception as exc:
    #     print(f"[redis] Resume cache get error: {exc}")

    results = await search_resume(firstName+"_"+lastName+"_Resume")

    if not results:
        raise HTTPException(status_code=404, detail="No resumes found")

    # Try to cache results, but don't fail if Redis is down
    # try:
    #     client = await get_redis()
    #     await client.set(cache_key, json.dumps([r.model_dump() for r in results]), ex=CACHE_EXPIRATION)
    # except Exception as exc:
    #     print(f"[redis] Resume cache set error: {exc}")

    return [r.model_dump() for r in results]


@app.get("/api/v1/analyzeResumes")
async def analyze_resumes(firstName: str | None = Query(default=None), lastName: str | None = Query(default=None)):
    if not firstName or not lastName:
        raise HTTPException(status_code=400, detail="Query parameters 'firstName' and 'lastName' are required")

    # 1. Search for resumes
    results = await search_resume(firstName + "_" + lastName + "_Resume")
    if not results:
        raise HTTPException(status_code=404, detail="No resumes found")

    # 2. Try fetching PDFs concurrently, but allow failures
    async def fetch_and_extract(url: str):
        try:
            pdf_bytes, _ = await fetch_pdf_bytes(url)
            text = extract_pdf_text_from_bytes(pdf_bytes)
            if not text.strip():
                return None, None
            return f"--- Document Source: {url} ---\n{text}\n", url
        except Exception as e:
            print(f"Failed to extract from {url}: {e}")
            return None, None

    # Try up to top 15 results to find at least some valid texts
    top_results_pool = results[:15]
    tasks = [fetch_and_extract(r.url) for r in top_results_pool]
    extracted_data = await asyncio.gather(*tasks)
    
    valid_texts = []
    used_documents = []
    
    for idx, (text, url) in enumerate(extracted_data):
        if text:
            valid_texts.append(text)
            used_documents.append(top_results_pool[idx])
            if len(valid_texts) >= 5: # Limit to 5 successful documents max
                break
    
    combined_text = "\n".join(valid_texts)
    
    # 3. If no text was extracted
    if not combined_text.strip():
        raise HTTPException(status_code=500, detail="Could not extract text from any of the resume links.")

    # 4. Analyze with Gemini
    analysis_result = await analyze_combined_resumes(combined_text)

    # 5. Return the analysis and the documents used
    return {
        "analysis": analysis_result,
        "documents": [r.model_dump() for r in used_documents]
    }


@app.get("/api/v1/proxyPdf")
async def proxy_pdf(url: str | None = Query(default=None)):
    if not url:
        raise HTTPException(status_code=400, detail="Missing URL parameter")

    try:
        data, content_type = await fetch_pdf_bytes(url)
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Content-Disposition": "inline",
            "Cache-Control": "public, max-age=3600",
        }
        return Response(content=data, media_type=content_type, headers=headers)
    except ValueError as exc:
        raise HTTPException(status_code=415, detail=str(exc)) from exc
    except httpx.HTTPStatusError as exc:  # type: ignore[name-defined]
        status = exc.response.status_code if exc.response is not None else 502
        raise HTTPException(status_code=status, detail=f"Failed to fetch PDF: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch PDF: {exc}") from exc


@app.get("/api/v1/preview")
async def preview_pdf(url: str | None = Query(default=None)):
    if not url:
        raise HTTPException(status_code=400, detail="Missing URL parameter")

    cache_key = f"preview:{url}"
    client = None
    try:
        client = await get_redis()
    except Exception as exc:
        print(f"[redis] Preview cache unavailable: {exc}")
    
    if client is not None:
        try:
            # Check cache first
            cached_preview = await client.get(cache_key)
            if cached_preview:
                return Response(
                    content=cached_preview,
                    media_type="image/jpeg",
                    headers={
                        "Access-Control-Allow-Origin": "*",
                        "Cache-Control": "public, max-age=3600",
                        "X-Cache": "hit",
                    },
                )
        except Exception as exc:
            print(f"[redis] Preview cache read error: {exc}")

    try:
        pdf_bytes, _ = await fetch_pdf_bytes(url)
        preview = first_page_preview_jpeg(pdf_bytes, scale=0.5, quality=55)

        # Cache preview for 24h
        if client is not None:
            try:
                await client.set(cache_key, preview, ex=60*60*24)
            except Exception as exc:
                print(f"[redis] Preview cache write error: {exc}")

        return Response(
            content=preview,
            media_type="image/jpeg",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=3600",
                "X-Cache": "miss",
            },
        )
    except ValueError as exc:
        raise HTTPException(status_code=415, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to render preview: {exc}") from exc


@app.post("/api/v1/books/save")
async def save_book(book: SavedBook, request: Request, response: Response):
    """Save a book to user's collection (Redis-backed, scoped by visitorId)"""
    # Get or create visitor ID
    visitor_id = request.cookies.get("visitorId")
    if not visitor_id:
        visitor_id = str(uuid.uuid4())
        response.set_cookie(
            key="visitorId",
            value=visitor_id,
            httponly=True,
            max_age=60 * 60 * 24 * 365,
            path="/",
            samesite="lax",
        )
    
    # Use current timestamp for savedAt
    book_data = book.model_copy(update={"savedAt": datetime.utcnow().isoformat()})
    
    try:
        client = await get_redis()
        cache_key = f"saved_books:{visitor_id}:{book.url}"
        await client.set(cache_key, json.dumps(book_data.model_dump()), ex=60*60*24*365)  # 1 year expiry
        return {"status": "saved", "url": book.url}
    except Exception as exc:
        print(f"[redis] Save book error: {exc}")
        return {"status": "error", "detail": "Failed to save book", "url": book.url}


@app.get("/api/v1/books/saved")
async def get_saved_books(request: Request):
    """Get all saved books for the current visitor (returns empty list if Redis is down)"""
    visitor_id = request.cookies.get("visitorId")
    
    if not visitor_id:
        return []
    
    try:
        client = await get_redis()
        # Get all keys matching pattern
        keys = await client.keys(f"saved_books:{visitor_id}:*")
        saved_books = []
        
        for key in keys:
            value = await client.get(key)
            if value:
                saved_books.append(json.loads(value))
        
        return saved_books
    except Exception as exc:
        print(f"[redis] Get saved books error: {exc}")
        return []


@app.delete("/api/v1/books/saved")
async def delete_saved_book(url: str = Query(...), request: Request = None):
    """Remove a book from user's saved collection"""
    visitor_id = request.cookies.get("visitorId") if request else None
    
    if not visitor_id:
        raise HTTPException(status_code=401, detail="Not authorized")
    
    try:
        client = await get_redis()
        cache_key = f"saved_books:{visitor_id}:{url}"
        await client.delete(cache_key)
        return {"status": "deleted", "url": url}
    except Exception as exc:
        print(f"[redis] Delete book error: {exc}")
        return {"status": "error", "detail": "Failed to delete book", "url": url}


@app.get("/api/v1/stats")
@app.get("/api/v1/stats/")
async def get_stats():
    """Get the number of unique visitors"""
    try:
        client = await get_redis()
        count = await client.scard("unique_visitors_crawler")
        return {"uniqueVisitors": count}
    except Exception as exc:
        print(f"[redis] Stats error: {exc}")
        return {"uniqueVisitors": 0}


@app.get("/api/v1/track")
@app.get("/api/v1/track/")
@app.post("/api/v1/track")
@app.post("/api/v1/track/")
async def track_visitor(request: Request, response: Response):
    """Track unique visitors and return the count"""
    try:
        client = await get_redis()
        existing = request.cookies.get("visitorId")

        if not existing:
            new_id = str(uuid.uuid4())
            await client.sadd("unique_visitors_crawler", new_id)
            count = await client.scard("unique_visitors_crawler")

            response.set_cookie(
                key="visitorId",
                value=new_id,
                httponly=True,
                path="/",
                max_age=60 * 60 * 24 * 365,
                samesite="none",
                secure=True
            )
            return {"newVisitor": True, "count": count}

        count = await client.scard("unique_visitors_crawler")
        return {"newVisitor": False, "count": count}
    except Exception as exc:
        print(f"[redis] Tracker error: {exc}")
        return {"newVisitor": False, "count": 0, "error": str(exc)}
