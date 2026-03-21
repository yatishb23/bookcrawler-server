import asyncio
import random
from urllib.parse import quote_plus, urlparse

import httpx
from bs4 import BeautifulSoup

from app.models import BookResult

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

SEARCH_ENGINES = [
    ("Brave", "https://search.brave.com/search?q={q}+filetype:pdf"),
    ("Google", "https://www.google.com/search?q={q}+filetype:pdf"),
    ("DuckDuckGo", "https://duckduckgo.com/?q={q}+filetype:pdf"),
]


def extract_title_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
        filename = path.split("/")[-1] if path else ""
        return filename.replace(".pdf", "").replace("_", " ").replace("-", " ").strip() or "Unknown Title"
    except Exception:
        return "Unknown Title"


def _calculate_relevance(title: str, search_query: str) -> float:
    """
    Calculate relevance score 0-1 based on title match to search query.
    Uses multiple matching strategies with weighted scoring.
    
    Scoring tiers:
    - 1.0: Exact match (case-insensitive)
    - 0.95: Query is substring of title
    - 0.90: Title starts with query
    - 0.85: All query words in title, in order
    - 0.80: All query words in title (any order)
    - 0.70: Most (>80%) query words match
    - 0.50: Half+ of query words match
    - 0.30: Any single word match or partial word match
    - 0.10: No match
    """
    if not search_query or not title:
        return 0.1
    
    title_lower = title.lower().strip()
    query_lower = search_query.lower().strip()
    
    # Exact match
    if title_lower == query_lower:
        return 1.0
    
    # Query is substring of title (case-insensitive)
    if query_lower in title_lower:
        return 0.95
    
    # Title starts with query
    if title_lower.startswith(query_lower):
        return 0.90
    
    # Split into words for word-based matching
    query_words = query_lower.split()
    title_words = title_lower.split()
    title_text = title_lower
    
    # No words to match
    if not query_words:
        return 0.1
    
    # Check if all query words appear in title in the same order
    def words_in_order():
        pos = 0
        for word in query_words:
            try:
                pos = title_text.index(word, pos)
                pos += len(word)
            except ValueError:
                return False
        return True
    
    if words_in_order():
        return 0.85
    
    # All query words present in title (any order)
    query_set = set(query_words)
    title_set = set(title_words)
    
    if query_set.issubset(title_set):
        return 0.80
    
    # Calculate matching words
    matching_words = query_set & title_set
    match_ratio = len(matching_words) / len(query_set)
    
    if match_ratio > 0.8:  # >80% of words match
        return 0.70
    elif match_ratio >= 0.5:  # 50%+ of words match
        return 0.50
    elif match_ratio > 0:  # Some words match
        return 0.30
    
    # Check for partial word matches (word starts with query word)
    for q_word in query_words:
        for t_word in title_words:
            if t_word.startswith(q_word[:3]) and len(q_word) >= 3:
                return 0.25
            if q_word in t_word:  # Query word is substring of title word
                return 0.30
    
    # No meaningful match
    return 0.10


def _parse_engine_results(html: str, engine: str, search_query: str = "") -> list[BookResult]:
    soup = BeautifulSoup(html, "lxml")
    results: list[BookResult] = []

    if engine == "Brave":
        for tag in soup.select('a[href*=".pdf"]'):
            link = tag.get("href")
            if not link or "brave.com" in link:
                continue
            title = tag.get_text(strip=True) or extract_title_from_url(link)
            relevance = _calculate_relevance(title, search_query) if search_query else 0.5
            results.append(BookResult(title=title, url=link, source="Brave", relevanceScore=relevance))

    elif engine == "Google":
        for tag in soup.select("a[href]"):
            link = tag.get("href")
            if not link or ".pdf" not in link or not link.startswith("http"):
                continue
            title = tag.get_text(strip=True) or extract_title_from_url(link)
            relevance = _calculate_relevance(title, search_query) if search_query else 0.5
            results.append(BookResult(title=title, url=link, source="Google", relevanceScore=relevance))

    elif engine == "DuckDuckGo":
        for tag in soup.select("a.result__a"):
            link = tag.get("href")
            if not link or ".pdf" not in link or not link.startswith("http"):
                continue
            title = tag.get_text(strip=True) or extract_title_from_url(link)
            relevance = _calculate_relevance(title, search_query) if search_query else 0.5
            results.append(BookResult(title=title, url=link, source="DuckDuckGo", relevanceScore=relevance))

    return results


async def search_book(book_name: str) -> list[BookResult]:
    timeout = httpx.Timeout(8.0)
    
    async def fetch_engine(client: httpx.AsyncClient, engine: str, template: str) -> list[BookResult]:
        try:
            ua = random.choice(USER_AGENTS)
            url = template.format(q=quote_plus(book_name))
            response = await client.get(
                url,
                headers={
                    "User-Agent": ua,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                },
            )
            response.raise_for_status()
            return _parse_engine_results(response.text, engine, book_name)
        except Exception as exc:
            print(f"[crawler] Error searching {engine}: {exc}")
            return []
    
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)) as client:
        tasks = [fetch_engine(client, engine, template) for engine, template in SEARCH_ENGINES]
        results_per_engine = await asyncio.gather(*tasks)
        all_results = [result for results in results_per_engine for result in results]

    # Deduplicate by URL, keeping the highest relevance score
    dedup_by_url: dict[str, BookResult] = {}
    for item in all_results:
        if item.url not in dedup_by_url or item.relevanceScore > dedup_by_url[item.url].relevanceScore:
            dedup_by_url[item.url] = item

    # Sort by relevance score (highest first)
    sorted_results = sorted(dedup_by_url.values(), key=lambda x: x.relevanceScore, reverse=True)
    return sorted_results
