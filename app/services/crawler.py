import asyncio
import random
import time
import hashlib
import json
import sqlite3
import importlib.util
from datetime import datetime, timedelta
from functools import lru_cache
from urllib.parse import quote_plus, urlparse, parse_qs, unquote
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict, is_dataclass
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiofiles
from fake_useragent import UserAgent
import cloudscraper

from app.models import BookResult
from app.config import settings

# ============= CONFIGURATION =============
class CrawlerConfig:
    """Centralized configuration for the crawler"""
    
    # Timeouts
    REQUEST_TIMEOUT = 20.0
    CONNECT_TIMEOUT = 12.0
    READ_TIMEOUT = 15.0
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_WAIT_MIN = 1.0
    RETRY_WAIT_MAX = 5.0
    
    # Rate limiting
    REQUESTS_PER_DOMAIN = 3  # per second
    BURST_MULTIPLIER = 2
    
    # Cache settings
    CACHE_DURATION_HOURS = 24
    CACHE_DB_PATH = "cache/search_cache.db"
    
    # Result limits
    MAX_RESULTS = 100
    MIN_RELEVANCE_SCORE = 0.2
    
    # Concurrency
    MAX_CONCURRENT_REQUESTS = 5
    
    # Proxy settings
    USE_PROXY = settings.use_proxy
    PROXY_LIST = settings.proxy_list
    
    # User agents
    USE_FAKE_UA = True
    CUSTOM_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]


# ============= CACHE MANAGER =============
class SearchCache:
    """Persistent cache for search results with expiration"""
    
    def __init__(self, db_path: str = CrawlerConfig.CACHE_DB_PATH):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS search_cache (
                    query_hash TEXT PRIMARY KEY,
                    query TEXT,
                    results TEXT,
                    created_at TIMESTAMP,
                    expires_at TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_expires_at 
                ON search_cache(expires_at)
            """)
            conn.commit()
    
    def get(self, query: str) -> Optional[List[Dict]]:
        """Get cached results if not expired"""
        query_hash = hashlib.md5(query.lower().encode()).hexdigest()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT results FROM search_cache WHERE query_hash = ? AND expires_at > ?",
                (query_hash, datetime.now())
            )
            row = cursor.fetchone()
            
            if row:
                return json.loads(row[0])
        return None
    
    def set(self, query: str, results: List[Dict]):
        """Cache results with expiration"""
        query_hash = hashlib.md5(query.lower().encode()).hexdigest()
        expires_at = datetime.now() + timedelta(hours=CrawlerConfig.CACHE_DURATION_HOURS)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO search_cache (query_hash, query, results, created_at, expires_at) VALUES (?, ?, ?, ?, ?)",
                (query_hash, query, json.dumps(results), datetime.now(), expires_at)
            )
            conn.commit()
    
    def cleanup(self):
        """Remove expired entries"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM search_cache WHERE expires_at <= ?", (datetime.now(),))
            conn.commit()


# ============= ENHANCED USER AGENT =============
class UserAgentManager:
    """Dynamic user agent management with rotation"""
    
    def __init__(self):
        self.ua = UserAgent() if CrawlerConfig.USE_FAKE_UA else None
        self.custom_agents = CrawlerConfig.CUSTOM_USER_AGENTS
        self._last_used = {}
    
    def get(self, domain: str = "") -> str:
        """Get a user agent, optionally rotating per domain"""
        if self.ua:
            try:
                return self.ua.random
            except:
                pass
        
        # Use custom agents as fallback
        return random.choice(self.custom_agents)
    
    def get_with_platform(self, platform: str = "desktop") -> str:
        """Get user agent for specific platform"""
        platform_agents = {
            "windows": [ua for ua in self.custom_agents if "Windows" in ua],
            "mac": [ua for ua in self.custom_agents if "Mac" in ua],
            "linux": [ua for ua in self.custom_agents if "Linux" in ua],
        }
        
        agents = platform_agents.get(platform, self.custom_agents)
        return random.choice(agents)


# ============= PROXY MANAGER =============
class ProxyManager:
    """Proxy rotation and management"""
    
    def __init__(self):
        self.proxies = CrawlerConfig.PROXY_LIST
    
    def get_proxy(self) -> Optional[Dict[str, str]]:
        """Get a random proxy in rotation if enabled, sometimes skipping proxy to randomize"""
        if not self.proxies or not CrawlerConfig.USE_PROXY:
            return None
        
        # 10% chance to not use a proxy at all for more organic traffic
        if random.random() < 0.1:
            return None

        proxy = random.choice(self.proxies)
        
        # Format proxy for httpx
        if proxy.startswith("http"):
            return {"http://": proxy, "https://": proxy}
        return {"http://": f"http://{proxy}", "https://": f"http://{proxy}"}
    
    def mark_failed(self, proxy: str):
        """Mark a proxy as failed (can be removed from rotation)"""
        # Implement failure tracking if needed
        pass


# ============= ENHANCED SEARCH ENGINES =============
@dataclass
class SearchEngine:
    """Search engine configuration"""
    name: str
    urls: List[str]  # Multiple URL patterns for fallback
    selectors: List[str]  # Multiple CSS selectors
    headers: Dict[str, str]  # Custom headers
    delay: float  # Delay between requests to this engine
    requires_js: bool = False  # Whether JS execution is needed
    
    def get_url(self, query: str, variation: int = 0) -> str:
        """Get URL with query, optionally using different variation"""
        url_template = self.urls[variation % len(self.urls)]
        return url_template.format(q=quote_plus(query))


SEARCH_ENGINES_CONFIG = [
    SearchEngine(
        name="Brave",
        urls=[
            "https://search.brave.com/search?q={q}+filetype:pdf",
            "https://search.brave.com/search?q={q}+pdf",
            "https://search.brave.com/search?q={q}+filetype%3Apdf",
        ],
        selectors=[
            'a[href*=".pdf"]',
            'a[href*="/url?q="][href*=".pdf"]',
            'div[class*="result"] a[href*=".pdf"]',
            'a[class*="result"]',
            'a[data-type="pdf"]',
            'a[href$=".pdf"]',
        ],
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        },
        delay=1.0,
    ),
    SearchEngine(
        name="Google",
        urls=[
            "https://www.google.com/search?q={q}+filetype:pdf&num=100",
            "https://www.google.com/search?q={q}+pdf&num=100",
            "https://www.google.com/search?q={q}+filetype%3Apdf&num=100",
        ],
        selectors=[
            'a[href*=".pdf"]',
            'a[href*="/url?q="][href*=".pdf"]',
            'div[class*="yuRUbf"] a',
            'h3 a',
            'a[class*="kCrYT"]',
            'a[data-ved]',
        ],
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        delay=1.5,
    ),
    SearchEngine(
        name="DuckDuckGo",
        urls=[
            "https://duckduckgo.com/html/?q={q}+filetype:pdf",
            "https://duckduckgo.com/html/?q={q}+pdf",
            "https://html.duckduckgo.com/html/?q={q}+filetype:pdf",
        ],
        selectors=[
            'a.result__a',
            'a[data-testid="result-title-a"]',
            'a[class*="result"]',
            'a[href*=".pdf"]',
            '.result a',
        ],
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        delay=1.0,
    ),
    SearchEngine(
        name="Bing",
        urls=[
            "https://www.bing.com/search?q={q}+filetype:pdf&count=50",
            "https://www.bing.com/search?q={q}+pdf&count=50",
            "https://www.bing.com/search?q={q}+filetype%3Apdf&count=50",
        ],
        selectors=[
            'a[href*=".pdf"]',
            'a[class*="tilk"]',
            'h2 a',
            'li[class*="b_algo"] a',
            'a[data-bm]',
        ],
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        delay=1.2,
    ),
    SearchEngine(
        name="Yahoo",
        urls=[
            "https://search.yahoo.com/search?p={q}+filetype:pdf",
            "https://search.yahoo.com/search?p={q}+pdf",
        ],
        selectors=[
            'a[href*=".pdf"]',
            'h3 a',
            'div[class*="algo"] a',
            'a[class*="title"]',
        ],
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        delay=1.0,
    ),
]


# ============= ENHANCED PARSER =============
class EnhancedParser:
    """Advanced HTML parser with multiple strategies"""
    
    def extract_pdf_links(self, html: str, engine: SearchEngine, search_query: str) -> List[BookResult]:
        """Extract PDF links with multiple fallback strategies"""
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen_urls = set()
        
        for selector in engine.selectors:
            for element in soup.select(selector):
                result = self._extract_from_element(element, engine.name, search_query)
                if result and result.url not in seen_urls:
                    seen_urls.add(result.url)
                    results.append(result)
        
        # If no results found, try more aggressive extraction
        if not results:
            results = self._aggressive_extraction(soup, engine.name, search_query)
        
        return results
    
    def _extract_from_element(self, element, source: str, search_query: str) -> Optional[BookResult]:
        """Extract book result from a single element"""
        try:
            # Extract link
            link = element.get("href")
            if not link:
                return None
            
            # Clean Google/Bing redirect URLs
            if link.startswith("/url?q="):
                parsed = urlparse(link)
                params = parse_qs(parsed.query)
                link = params.get('q', [None])[0]
                if not link:
                    return None
            
            # Ensure it's a PDF link
            if ".pdf" not in link.lower():
                return None
            
            # Validate URL
            if not link.startswith(("http://", "https://")):
                return None
            
            # Extract title with multiple strategies
            title = self._extract_title(element, link)
            
            # Calculate relevance
            relevance = self._calculate_relevance(title, search_query)
            
            return BookResult(
                title=title[:200],
                url=link,
                source=source,
                relevanceScore=relevance
            )
        except Exception:
            return None
    
    def _extract_title(self, element, url: str) -> str:
        """Extract title with multiple fallback strategies"""
        # Strategy 1: Direct text
        title = element.get_text(strip=True)
        if title:
            return title
        
        # Strategy 2: Parent element
        if element.parent:
            title = element.parent.get_text(strip=True)
            if title and len(title) < 100:  # Reasonable title length
                return title
        
        # Strategy 3: Find nearest heading
        heading = element.find_previous(['h1', 'h2', 'h3', 'h4'])
        if heading:
            title = heading.get_text(strip=True)
            if title:
                return title
        
        # Strategy 4: Extract from URL
        return self._extract_title_from_url(url)
    
    def _extract_title_from_url(self, url: str) -> str:
        """Extract readable title from URL"""
        try:
            parsed = urlparse(url)
            path = parsed.path
            
            # Get filename
            filename = path.split("/")[-1] if path else ""
            filename = unquote(filename)
            
            # Clean up
            title = filename.replace(".pdf", "").replace("_", " ").replace("-", " ").strip()
            
            # If title is empty, try last meaningful part of path
            if not title:
                parts = [p for p in path.split("/") if p and not p.isdigit()]
                if parts:
                    title = parts[-1].replace(".pdf", "").replace("_", " ").replace("-", " ").strip()
            
            return title or "Unknown Title"
        except Exception:
            return "Unknown Title"
    
    def _aggressive_extraction(self, soup, source: str, search_query: str) -> List[BookResult]:
        """Aggressive extraction when standard selectors fail"""
        results = []
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '.pdf' in href.lower():
                # Try to find any text nearby
                title = link.get_text(strip=True)
                if not title:
                    # Look for sibling text
                    next_elem = link.find_next_sibling()
                    if next_elem:
                        title = next_elem.get_text(strip=True)
                
                if not title:
                    # Look for any text in parent
                    parent = link.parent
                    if parent:
                        title = parent.get_text(strip=True)[:100]
                
                if title:
                    relevance = self._calculate_relevance(title, search_query)
                    results.append(BookResult(
                        title=title[:200],
                        url=href,
                        source=f"{source}_Aggressive",
                        relevanceScore=relevance
                    ))
        
        return results
    
    def _calculate_relevance(self, title: str, search_query: str) -> float:
        """Advanced relevance scoring with multiple strategies"""
        if not search_query or not title:
            return 0.1
        
        title_lower = title.lower().strip()
        query_lower = search_query.lower().strip()
        
        # Exact match
        if title_lower == query_lower:
            return 1.0
        
        # Query is substring
        if query_lower in title_lower:
            return 0.95
        
        # Title starts with query
        if title_lower.startswith(query_lower):
            return 0.90
        
        # Word-based matching
        query_words = set(query_lower.split())
        title_words = set(title_lower.split())
        
        if not query_words:
            return 0.5
        
        # Calculate match ratio
        matching_words = query_words & title_words
        match_ratio = len(matching_words) / len(query_words)
        
        # Partial word matching
        partial_matches = 0
        for q_word in query_words:
            if len(q_word) <= 3:
                continue
            for t_word in title_words:
                if len(t_word) <= 3:
                    continue
                if q_word in t_word or t_word in q_word:
                    partial_matches += 0.5
                    break
        
        # Sequence matching
        sequence_score = 0
        query_word_list = list(query_words)
        title_word_list = list(title_words)
        
        if len(query_word_list) >= 2 and len(title_word_list) >= 2:
            # Check if query words appear in order
            match_positions = []
            for q_word in query_word_list:
                for i, t_word in enumerate(title_word_list):
                    if q_word == t_word:
                        match_positions.append(i)
                        break
            
            if len(match_positions) >= 2:
                if all(match_positions[i] < match_positions[i+1] for i in range(len(match_positions)-1)):
                    sequence_score = 0.15
        
        # Calculate final score
        base_score = match_ratio + (partial_matches / len(query_words))
        final_score = min(0.95, base_score + sequence_score)
        
        # Adjust based on length similarity
        length_ratio = min(1.0, len(title_words) / (len(query_words) * 1.5))
        final_score *= (0.8 + (length_ratio * 0.2))
        
        # Cap at different levels
        if final_score >= 0.9:
            return 0.85
        elif final_score >= 0.7:
            return 0.75
        elif final_score >= 0.5:
            return 0.60
        elif final_score >= 0.3:
            return 0.40
        elif final_score > 0:
            return 0.25
        
        return 0.10


# ============= ENHANCED HTTP CLIENT =============
class EnhancedHTTPClient:
    """Advanced HTTP client with retries, proxy rotation, and rate limiting"""
    
    def __init__(self):
        self.ua_manager = UserAgentManager()
        self.proxy_manager = ProxyManager()
        self.parser = EnhancedParser()
        self.cache = SearchCache()
        self._rate_limiter = {}
        self._last_request_time = {}
    
    def _can_make_request(self, domain: str) -> bool:
        """Rate limiting per domain"""
        now = time.time()
        last_time = self._last_request_time.get(domain, 0)
        
        # Allow requests per second
        min_interval = 1.0 / CrawlerConfig.REQUESTS_PER_DOMAIN
        if now - last_time < min_interval:
            return False
        
        self._last_request_time[domain] = now
        return True
    
    @retry(
        stop=stop_after_attempt(CrawlerConfig.MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=CrawlerConfig.RETRY_WAIT_MIN, max=CrawlerConfig.RETRY_WAIT_MAX),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError, httpx.ReadError))
    )
    async def fetch(self, url: str, engine: SearchEngine, client: httpx.AsyncClient) -> Optional[str]:
        """Fetch URL with retries and error handling"""
        domain = urlparse(url).netloc
        
        # Rate limiting
        if not self._can_make_request(domain):
            await asyncio.sleep(random.uniform(0.5, 1.0))
        
        try:
            # Prepare headers
            headers = {
                "User-Agent": self.ua_manager.get(domain),
                **engine.headers,
            }
            
            # Add random parameters to avoid caching
            random_param = f"&_={random.randint(1000000, 9999999)}"
            url_with_random = url + random_param
            
            response = await client.get(
                url_with_random,
                headers=headers,
                timeout=httpx.Timeout(
                    CrawlerConfig.REQUEST_TIMEOUT,
                    connect=CrawlerConfig.CONNECT_TIMEOUT,
                    read=CrawlerConfig.READ_TIMEOUT,
                ),
                follow_redirects=True,
            )
            
            if response.status_code == 200:
                return response.text
            elif response.status_code == 429:
                # Rate limited - wait longer
                wait_time = random.uniform(5, 15)
                await asyncio.sleep(wait_time)
                raise httpx.TooManyRequests("Rate limited")
            else:
                print(f"[crawler] {engine.name} returned status {response.status_code}")
                return None
                
        except Exception as e:
            print(f"[crawler] Error fetching {url}: {e}")
            raise
    
    async def search_engine(self, engine: SearchEngine, query: str, client: httpx.AsyncClient) -> List[BookResult]:
        """Search a single engine with multiple URL variations"""
        all_results = []
        
        for url_variation in range(len(engine.urls)):
            # Delay between variations
            if url_variation > 0:
                await asyncio.sleep(engine.delay * random.uniform(0.5, 1.5))
            
            url = engine.get_url(query, url_variation)
            
            try:
                html = await self.fetch(url, engine, client)
                if html:
                    results = self.parser.extract_pdf_links(html, engine, query)
                    if results:
                        all_results.extend(results)
                        # If we found results, we can stop trying variations
                        if len(results) > 5:
                            break
            except Exception as e:
                print(f"[crawler] Error with {engine.name} variation {url_variation}: {e}")
                continue
        
        # Deduplicate results from same engine
        seen_urls = set()
        unique_results = []
        for result in all_results:
            if result.url not in seen_urls:
                seen_urls.add(result.url)
                unique_results.append(result)
        
        return unique_results


# ============= RESULT PROCESSOR =============
class ResultProcessor:
    """Process and enhance search results"""
    
    def __init__(self):
        self.cache = SearchCache()
    
    def deduplicate(self, results: List[BookResult]) -> List[BookResult]:
        """Advanced deduplication with URL normalization"""
        url_map = {}
        
        for result in results:
            # Normalize URL
            normalized_url = self._normalize_url(result.url)
            
            if normalized_url not in url_map:
                url_map[normalized_url] = result
            else:
                # Keep the one with higher relevance
                existing = url_map[normalized_url]
                if result.relevanceScore > existing.relevanceScore:
                    url_map[normalized_url] = result
        
        return list(url_map.values())
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL for deduplication"""
        parsed = urlparse(url)
        
        # Remove query parameters that don't affect content
        # Keep only essential params (like 'v' for YouTube, etc.)
        # For PDFs, most params are tracking
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        
        # Remove trailing slash
        normalized = normalized.rstrip('/')
        
        # Remove common tracking parameters
        normalized = normalized.split('?')[0].split('#')[0]
        
        return normalized.lower()
    
    def sort_by_relevance(self, results: List[BookResult]) -> List[BookResult]:
        """Sort results with tie-breaking"""
        return sorted(
            results,
            key=lambda x: (x.relevanceScore, len(x.title), x.source),
            reverse=True
        )
    
    def filter_low_relevance(self, results: List[BookResult]) -> List[BookResult]:
        """Remove very low relevance results"""
        return [
            r for r in results 
            if r.relevanceScore >= CrawlerConfig.MIN_RELEVANCE_SCORE
        ]
    
    def enrich_results(self, results: List[BookResult], query: str) -> List[BookResult]:
        """Enrich results with additional metadata"""
        enriched = []
        
        for result in results:
            # Boost results that exactly match the query
            if query.lower() in result.title.lower():
                result.relevanceScore = min(1.0, result.relevanceScore + 0.1)
            
            # Boost results from trusted sources
            trusted_domains = ['arxiv.org', 'acm.org', 'ieee.org', 'springer.com', 'science.org']
            if any(domain in result.url for domain in trusted_domains):
                result.relevanceScore = min(1.0, result.relevanceScore + 0.05)
            
            enriched.append(result)
        
        return enriched


# ============= MAIN CRAWLER CLASS =============
class BookCrawler:
    """Main crawler orchestrator"""
    
    def __init__(self):
        self.http_client = EnhancedHTTPClient()
        self.processor = ResultProcessor()
        self.cache = SearchCache()
    
    async def search(self, book_name: str) -> List[BookResult]:
        """Main search method with full pipeline"""

        # Keep input as-is; only reject truly empty input.
        if book_name == "":
            return []
        
        # Check cache
        cached = self.cache.get(book_name)
        if cached is not None:
            print(f"[crawler] Using cached results for '{book_name}'")
            return [BookResult(**r) for r in cached]
        
        # Configure HTTP client with connection pooling
        limits = httpx.Limits(
            max_connections=CrawlerConfig.MAX_CONCURRENT_REQUESTS * 2,
            max_keepalive_connections=CrawlerConfig.MAX_CONCURRENT_REQUESTS,
            keepalive_expiry=30
        )
        
        http2_enabled = importlib.util.find_spec("h2") is not None
        if not http2_enabled:
            print("[crawler] 'h2' not installed. Falling back to HTTP/1.1.")

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(CrawlerConfig.REQUEST_TIMEOUT),
            limits=limits,
            http2=http2_enabled,
            verify=True  # Enable SSL verification
        ) as client:
            
            # Search all engines with concurrency control
            semaphore = asyncio.Semaphore(CrawlerConfig.MAX_CONCURRENT_REQUESTS)
            
            async def search_with_semaphore(engine):
                async with semaphore:
                    # Random delay before each engine to avoid patterns
                    await asyncio.sleep(random.uniform(0.2, 0.8))
                    return await self.http_client.search_engine(engine, book_name, client)
            
            tasks = [search_with_semaphore(engine) for engine in SEARCH_ENGINES_CONFIG]
            engine_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect all results
            all_results = []
            for results in engine_results:
                if isinstance(results, list):
                    all_results.extend(results)
                elif isinstance(results, Exception):
                    print(f"[crawler] Engine search failed: {results}")
        
        # Process results
        if not all_results:
            print(f"[crawler] No results found for '{book_name}'")
            return []
        
        # Deduplicate
        deduped = self.processor.deduplicate(all_results)
        
        # Filter low relevance
        filtered = self.processor.filter_low_relevance(deduped)
        
        # Enrich
        enriched = self.processor.enrich_results(filtered, book_name)
        
        # Sort
        sorted_results = self.processor.sort_by_relevance(enriched)
        
        # Limit results
        final_results = sorted_results[:CrawlerConfig.MAX_RESULTS]
        
        # Cache results (BookResult is a Pydantic model, but keep safe fallbacks).
        cacheable = [
            r.model_dump() if hasattr(r, "model_dump")
            else asdict(r) if is_dataclass(r)
            else dict(r) if isinstance(r, dict)
            else r.__dict__
            for r in final_results
        ]
        self.cache.set(book_name, cacheable)
        
        print(f"[crawler] Found {len(final_results)} unique PDF results for '{book_name}'")
        
        return final_results


# ============= CLEANUP UTILITY =============
async def cleanup_cache():
    """Clean up expired cache entries"""
    cache = SearchCache()
    cache.cleanup()
    print("[crawler] Cache cleanup completed")


# ============= MAIN FUNCTION =============
async def search_book(book_name: str) -> List[BookResult]:
    """Public API function"""
    crawler = BookCrawler()
    
    # Run cleanup occasionally (every 100 searches)
    if random.random() < 0.01:  # 1% chance
        await cleanup_cache()
    
    return await crawler.search(book_name)


# ============= BACKWARD COMPATIBILITY =============
# Export the main function
__all__ = ['search_book', 'BookResult', 'cleanup_cache']