from urllib.parse import urlparse
from datetime import datetime

import fitz
import httpx

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


async def fetch_pdf_bytes(url: str) -> tuple[bytes, str]:
    parsed = urlparse(url)
    referer = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""

    timeout = httpx.Timeout(15.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, verify=False) as client:
        response = await client.get(
            url,
            headers={
                "User-Agent": DEFAULT_UA,
                "Accept": "application/pdf, application/octet-stream, */*",
                "Referer": referer,
                "Connection": "keep-alive",
            },
        )
        response.raise_for_status()

    body = response.content
    content_type = response.headers.get("content-type", "application/pdf")

    is_html = "text/html" in content_type.lower()
    has_pdf_header = body[:4] == b"%PDF"

    if is_html and not has_pdf_header:
        raise ValueError("Link returned a webpage, not a PDF file")

    try:
        with fitz.open(stream=body, filetype="pdf") as doc:
            if len(doc) <= 50:
                raise ValueError("PDF must have more than 50 pages")
    except fitz.FileDataError:
        raise ValueError("Invalid PDF file structure")
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Error parsing PDF: {exc}")

    return body, content_type


def first_page_preview_jpeg(pdf_bytes: bytes, scale: float = 0.5, quality: int = 55) -> bytes:
    """Generate JPEG preview from first PDF page. Optimized for speed over quality."""
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        if len(doc) == 0:
            raise ValueError("PDF has no pages")

        page = doc.load_page(0)
        # Lower scale + quality = faster rendering
        matrix = fitz.Matrix(scale, scale)
        pix = page.get_pixmap(matrix=matrix, alpha=False)

        img_bytes = pix.tobytes("jpg", jpg_quality=quality)
        return img_bytes


def extract_pdf_metadata(pdf_bytes: bytes) -> dict:
    """Extract metadata (author, title, year) from PDF."""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            metadata = doc.metadata or {}
            
            # Extract fields
            title = metadata.get("title") or None
            author = metadata.get("author") or None
            
            # Extract year from creation date
            year = None
            creation_date = metadata.get("creationDate")
            if creation_date:
                try:
                    # Parse 'D:20241201120000Z' format
                    if isinstance(creation_date, str):
                        if creation_date.startswith("D:"):
                            year = int(creation_date[2:6])
                        else:
                            # Try to parse as ISO datetime
                            dt = datetime.fromisoformat(creation_date.replace("Z", "+00:00"))
                            year = dt.year
                except (ValueError, IndexError):
                    pass
            
            return {
                "title": title,
                "author": author,
                "year": year,
            }
    except Exception as e:
        print(f"[pdf_service] Error extracting metadata: {e}")
        return {"title": None, "author": None, "year": None}
