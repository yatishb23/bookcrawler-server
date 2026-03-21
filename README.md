# FastAPI Backend (Separate Service)

This folder contains a standalone FastAPI backend equivalent of your Next.js API routes, plus server-side first-page PDF preview generation.

## Endpoints

- `GET /health`
- `GET /api/v1/getBooks?q=<book name>`
- `GET /api/v1/proxyPdf?url=<pdf url>`
- `GET /api/v1/preview?url=<pdf url>` -> returns first-page preview image (`image/jpeg`)
- `GET /api/v1/track`
- `GET /api/v1/stats`

## Setup

1. Create env file:

```bash
cp .env.example .env
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run server:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Required env vars

- `REDIS_HOST`
- `REDIS_PORT`
- `REDIS_PASSWORD` (optional)
- `FRONTEND_ORIGIN`

## Frontend integration notes

If you want Next.js frontend to use this backend directly, set a backend base URL in frontend env and call:

- `${BACKEND_BASE_URL}/api/v1/getBooks?q=...`
- `${BACKEND_BASE_URL}/api/v1/preview?url=...`

For preview rendering, you can replace client-side `pdfjs-dist` thumbnail generation with a direct image src:

```tsx
<img src={`${BACKEND_BASE_URL}/api/v1/preview?url=${encodeURIComponent(book.url)}`} alt={book.title} />
```
