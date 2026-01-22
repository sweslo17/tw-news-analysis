"""SETN (三立新聞) news crawler implementations."""

import asyncio
import json
import random
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from crawlers.base import ArticleData, BaseArticleCrawler, BaseListCrawler, CrawlerResult

# User-Agent list for rotation to avoid being banned
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
]


def get_random_headers(referer: str | None = None) -> dict[str, str]:
    """Generate headers with random User-Agent."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def is_setn_url(url: str) -> bool:
    """Check if URL belongs to setn.com domain (including subdomains)."""
    if not url:
        return False
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    return host.endswith("setn.com") or host == "setn.com"


class SetnListCrawler(BaseListCrawler):
    """
    List crawler for SETN (三立新聞) news.

    Fetches article URLs from SETN's viewall page with pagination.
    Supports all setn.com subdomains (star, health, odd, fuhouse, etc.).
    """

    @property
    def name(self) -> str:
        return "setn_list"

    @property
    def display_name(self) -> str:
        return "SETN - List"

    @property
    def source(self) -> str:
        return "SETN"

    @property
    def default_interval_minutes(self) -> int:
        return 15

    @property
    def max_pages(self) -> int:
        """Maximum number of pages to crawl."""
        return 10

    def _extract_urls_from_html(self, soup: BeautifulSoup) -> set[str]:
        """Extract article URLs from viewall page HTML."""
        urls = set()

        # Find all news items in the list
        for item in soup.select(".col-sm-12.newsItems"):
            # Find the article link within the title
            title_h3 = item.select_one("h3.view-li-title")
            if not title_h3:
                continue

            link = title_h3.find("a", href=True)
            if not link:
                continue

            href = link["href"]

            # Convert relative URLs to absolute
            if href.startswith("/"):
                href = urljoin("https://www.setn.com", href)

            # Only accept setn.com URLs (including subdomains)
            if is_setn_url(href):
                # Clean up URL - remove utm parameters but keep NewsID
                if "?" in href:
                    base_url = href.split("?")[0]
                    # Parse query string to keep only NewsID
                    query_params = href.split("?")[1] if "?" in href else ""
                    news_id_match = re.search(r"NewsID=(\d+)", query_params)
                    if news_id_match and "/News.aspx" in base_url:
                        href = f"{base_url}?NewsID={news_id_match.group(1)}"
                    elif "/news/" in href.lower():
                        # For subdomain URLs like star.setn.com/news/1234567
                        href = base_url

                urls.add(href)

        return urls

    async def get_article_urls(self) -> list[str]:
        """Fetch article URLs from SETN's viewall list."""
        all_urls: set[str] = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            for page in range(1, self.max_pages + 1):
                page_url = f"https://www.setn.com/viewall.aspx?p={page}"
                headers = get_random_headers(referer="https://www.setn.com/")

                try:
                    response = await client.get(page_url, headers=headers)

                    # Handle 429 Too Many Requests
                    if response.status_code == 429:
                        print(f"[{self.name}] Rate limited on page {page}, backing off...")
                        await asyncio.sleep(10)
                        break

                    response.raise_for_status()
                    html = response.text

                    # Parse and extract URLs
                    soup = BeautifulSoup(html, "html.parser")
                    page_urls = self._extract_urls_from_html(soup)
                    all_urls.update(page_urls)

                    print(f"[{self.name}] Page {page}: found {len(page_urls)} URLs")

                except httpx.HTTPStatusError as e:
                    print(f"[{self.name}] HTTP error on page {page}: {e}")
                    if e.response.status_code == 429:
                        await asyncio.sleep(10)
                        break
                except Exception as e:
                    print(f"[{self.name}] Error fetching page {page}: {e}")

                # Random delay between pages to avoid being banned
                if page < self.max_pages:
                    await asyncio.sleep(random.uniform(1, 3))

        return list(all_urls)

    async def on_success(self, result: CrawlerResult) -> None:
        """Log successful crawl."""
        print(f"[{self.name}] Discovered {result.items_processed} URLs")


class SetnArticleCrawler(BaseArticleCrawler):
    """
    Article crawler for SETN (三立新聞) news.

    Fetches and parses individual article pages from setn.com and its subdomains.
    """

    @property
    def name(self) -> str:
        return "setn_article"

    @property
    def display_name(self) -> str:
        return "SETN - Article"

    @property
    def source(self) -> str:
        return "SETN"

    @property
    def default_interval_minutes(self) -> int:
        return 5

    @property
    def default_timeout_seconds(self) -> int:
        """Longer timeout for fetching many articles (30 minutes)."""
        return 1800

    @property
    def batch_size(self) -> int:
        """0 means no limit - fetch all pending URLs."""
        return 0

    def _parse_json_ld(self, soup: BeautifulSoup) -> dict:
        """Parse JSON-LD NewsArticle script from page."""
        script_tags = soup.find_all("script", type="application/ld+json")
        for script in script_tags:
            try:
                if script.string:
                    # Remove control characters that may cause JSON parsing to fail
                    json_str = script.string
                    json_str = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", json_str)
                    data = json.loads(json_str)

                    # Handle both single object and array
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and item.get("@type") == "NewsArticle":
                                return item
                    elif isinstance(data, dict) and data.get("@type") == "NewsArticle":
                        return data
            except (json.JSONDecodeError, ValueError):
                continue
        return {}

    def _parse_meta_tags(self, soup: BeautifulSoup) -> dict[str, str]:
        """Parse relevant meta tags from page."""
        result = {}

        for meta in soup.find_all("meta"):
            name = meta.get("name", "").lower()
            prop = meta.get("property", "").lower()
            content = meta.get("content", "")

            if not content:
                continue

            # Standard meta names
            if name == "news_keywords":
                result["keywords"] = content
            elif name == "description":
                result["description"] = content
            elif name == "author":
                result["author"] = content

            # Open Graph and article properties
            if prop == "article:section":
                result["section"] = content
            elif prop == "article:published_time":
                result["published_time"] = content
            elif prop == "article:modified_time":
                result["modified_time"] = content
            elif prop == "og:description" and "description" not in result:
                result["description"] = content
            elif prop == "og:image" and "og_image" not in result:
                result["og_image"] = content

        return result

    def _extract_author(self, soup: BeautifulSoup, json_ld: dict, meta: dict | None = None) -> str | None:
        """Extract author name from JSON-LD, meta tags, or content."""
        # Try JSON-LD first (most reliable for main site)
        author = json_ld.get("author")
        if author:
            if isinstance(author, dict):
                name = author.get("name")
                if name:
                    return name
            if isinstance(author, str):
                return author

        # Try to find author from first paragraph in content
        content_div = soup.find("div", id="ckuse")
        if content_div:
            first_p = content_div.find("p")
            if first_p:
                text = first_p.get_text(strip=True)
                # Pattern: "XXX中心／YYY報導" or "記者XXX／報導"
                match = re.search(r"(?:記者)?([^／/]+)[／/]([^報]*報導)", text)
                if match:
                    return match.group(1).strip()
                # Pattern for health subdomain: "圖、文／XXX授權" or "文／XXX"
                match = re.search(r"(?:圖、)?文[／/](.+?)(?:授權|提供|$)", text)
                if match:
                    return match.group(1).strip()

        # Fallback to meta author tag (common on subdomains)
        if meta and meta.get("author"):
            author_meta = meta["author"]
            # Skip generic "三立新聞網" if we can find a more specific author
            if author_meta and author_meta != "三立新聞網":
                return author_meta

        return None

    def _normalize_image_url(self, src: str) -> str | None:
        """Normalize image URL to full https URL."""
        if not src:
            return None
        # Handle protocol-relative URLs
        if src.startswith("//"):
            src = "https:" + src
        # Accept SETN image URLs
        if "attach.setn.com" in src or "setn.com" in src:
            return src
        return None

    def _extract_category_from_url(self, url: str) -> str | None:
        """Extract category from SETN subdomain (fallback when meta tags unavailable)."""
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        # Map subdomains to categories
        subdomain_category_map = {
            "health.setn.com": "健康",
            "star.setn.com": "娛樂",
            "odd.setn.com": "新奇",
            "fuhouse.setn.com": "房產",
            "travel.setn.com": "旅遊",
            "money.setn.com": "財經",
            "sports.setn.com": "體育",
        }

        return subdomain_category_map.get(host)

    def _extract_images(self, soup: BeautifulSoup, meta: dict) -> list[str] | None:
        """Extract image URLs from article."""
        images = []
        seen = set()

        # Find images in article content
        content_div = soup.find("div", id="ckuse")
        if content_div:
            # Find figure > picture > img or just img
            for figure in content_div.find_all("figure"):
                img = figure.find("img", src=True)
                if img:
                    url = self._normalize_image_url(img.get("src", ""))
                    if url and url not in seen:
                        images.append(url)
                        seen.add(url)

            # Also check for standalone images
            for img in content_div.find_all("img", src=True):
                url = self._normalize_image_url(img.get("src", ""))
                if url and url not in seen:
                    images.append(url)
                    seen.add(url)

        # Fallback to og:image
        if "og_image" in meta:
            url = self._normalize_image_url(meta["og_image"])
            if url and url not in seen:
                images.append(url)
                seen.add(url)

        return images if images else None

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract article content from page with image placeholders."""
        content_parts = []

        # Try multiple selectors for content
        content_div = soup.find("div", id="ckuse")
        if not content_div:
            content_div = soup.find("div", class_="news-content")
        if not content_div:
            return ""

        # Find article or Content1 within ckuse
        article = content_div.find("article")
        if article:
            content_container = article.find("div", id="Content1")
            if not content_container:
                content_container = article
        else:
            content_container = content_div

        # Iterate through paragraphs and figures
        for element in content_container.find_all(["p", "figure"]):
            if element.name == "p":
                # Check if paragraph contains an image
                img = element.find("img", src=True)
                if img:
                    url = self._normalize_image_url(img.get("src", ""))
                    if url:
                        content_parts.append(f"[{url}]")

                # Get text content, skip if it's just author line
                text = element.get_text(strip=True)
                if text and not re.match(r"^(?:記者)?[^／/]+[／/].*報導$", text):
                    content_parts.append(text)

            elif element.name == "figure":
                # Handle figure elements with images
                img = element.find("img", src=True)
                if img:
                    url = self._normalize_image_url(img.get("src", ""))
                    if url:
                        content_parts.append(f"[{url}]")

        return "\n\n".join(content_parts)

    def _parse_published_at(self, meta: dict, json_ld: dict, soup: BeautifulSoup) -> datetime | None:
        """Parse published_at from meta, JSON-LD, or HTML, convert to UTC for storage."""
        from zoneinfo import ZoneInfo

        # Priority: meta > JSON-LD > HTML time element
        date_str = meta.get("published_time") or json_ld.get("datePublished")

        if not date_str:
            # Try HTML time element (multiple class names for different subdomains)
            time_elem = soup.find("time", class_="page_date")
            if not time_elem:
                time_elem = soup.find("time", class_="pageDate")  # health subdomain uses camelCase
            if time_elem:
                date_str = time_elem.get_text(strip=True)
                # Format: "2026/01/13 17:12"
                try:
                    dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
                    # Assume Taiwan timezone
                    tw_tz = ZoneInfo("Asia/Taipei")
                    dt = dt.replace(tzinfo=tw_tz)
                    dt_utc = dt.astimezone(ZoneInfo("UTC"))
                    return dt_utc.replace(tzinfo=None)
                except ValueError:
                    pass
            return None

        try:
            # Handle ISO format with timezone
            date_str = date_str.replace("Z", "+00:00")
            # Handle format like "2026-01-13 18:30 +00:00" (space before timezone)
            date_str = re.sub(r"\s+([+-]\d{2}:\d{2})$", r"\1", date_str)

            dt = datetime.fromisoformat(date_str)
            # Convert to UTC naive datetime for consistent storage
            if dt.tzinfo is not None:
                dt_utc = dt.astimezone(ZoneInfo("UTC"))
                return dt_utc.replace(tzinfo=None)
            return dt
        except ValueError:
            return None

    def _parse_keywords(self, meta: dict, json_ld: dict) -> list[str] | None:
        """Parse keywords from meta tags or JSON-LD."""
        # Try meta keywords first
        keywords_str = meta.get("keywords")
        if keywords_str:
            keywords = [k.strip() for k in keywords_str.split(",") if k.strip()]
            if keywords:
                return keywords

        # Fallback to JSON-LD keywords
        keywords = json_ld.get("keywords")
        if keywords:
            if isinstance(keywords, str):
                keywords = [k.strip() for k in keywords.split(",") if k.strip()]
            if isinstance(keywords, list):
                return keywords if keywords else None

        return None

    def parse_html(self, raw_html: str, url: str) -> ArticleData:
        """Parse article data from raw HTML without making network requests."""
        soup = BeautifulSoup(raw_html, "html.parser")

        # Parse structured data
        json_ld = self._parse_json_ld(soup)
        meta = self._parse_meta_tags(soup)

        # Extract title - try multiple sources
        title = ""
        h1_tag = soup.find("h1", class_="news-title-3")
        if h1_tag:
            title = h1_tag.get_text(strip=True)
        elif json_ld.get("headline"):
            title = json_ld["headline"]
        else:
            # Fallback: try subdomain selectors (health, star, etc.)
            title_div = soup.find("div", class_="newsTitle")
            if title_div:
                title = title_div.get_text(strip=True)
            else:
                # Last fallback to first h1
                h1_tag = soup.find("h1")
                if h1_tag:
                    title = h1_tag.get_text(strip=True)

        # Extract fields using helper methods
        author = self._extract_author(soup, json_ld, meta)
        published_at = self._parse_published_at(meta, json_ld, soup)
        category = meta.get("section") or json_ld.get("articleSection")
        # Fallback: extract category from subdomain
        if not category:
            category = self._extract_category_from_url(url)
        summary = meta.get("description")
        tags = self._parse_keywords(meta, json_ld)
        content = self._extract_content(soup)
        images = self._extract_images(soup, meta)

        return ArticleData(
            url=url,
            title=title,
            content=content,
            summary=summary,
            author=author,
            category=category,
            sub_category=None,
            tags=tags,
            published_at=published_at,
            images=images,
        )

    async def fetch_article(self, url: str) -> ArticleData:
        """Fetch and parse a single SETN article."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Determine referer based on URL domain
            parsed = urlparse(url)
            referer = f"{parsed.scheme}://{parsed.netloc}/"

            headers = get_random_headers(referer=referer)
            response = await client.get(url, headers=headers, follow_redirects=True)

            # Handle 429 Too Many Requests with backoff
            if response.status_code == 429:
                await asyncio.sleep(10)
                response = await client.get(url, headers=headers, follow_redirects=True)

            response.raise_for_status()
            raw_html = response.text

        # Parse the HTML using the shared method
        article = self.parse_html(raw_html, url)
        article.raw_html = raw_html

        # Add delay between articles to avoid being banned
        await asyncio.sleep(random.uniform(0.5, 2))

        return article

    async def on_success(self, result: CrawlerResult) -> None:
        """Log successful crawl."""
        print(f"[{self.name}] Fetched {result.new_items}/{result.items_processed} articles")

    async def on_failure(self, result: CrawlerResult) -> None:
        """Log failed crawl."""
        print(f"[{self.name}] Failed: {result.error}")
