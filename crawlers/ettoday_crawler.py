"""ETtoday news crawler implementations."""

import asyncio
import json
import random
import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from crawlers.base import ArticleData, BaseArticleCrawler, BaseListCrawler, CrawlerResult

# User-Agent list for rotation to avoid being banned
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def get_random_headers(referer: str | None = None, ajax: bool = False) -> dict[str, str]:
    """Generate headers with random User-Agent.

    Args:
        referer: Optional referer URL.
        ajax: If True, add X-Requested-With header for AJAX requests.
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    if referer:
        headers["Referer"] = referer
    if ajax:
        headers["X-Requested-With"] = "XMLHttpRequest"
    return headers


# List of ETtoday subdomains
ETTODAY_SUBDOMAINS = [
    "www",      # 一般新聞
    "star",     # 娛樂
    "finance",  # 財經
    "sports",   # 體育
    "health",   # 健康
    "pets",     # 寵物
    "travel",   # 旅遊
    "house",    # 房產
    "fashion",  # 時尚
    "speed",    # 車雲
    "game",     # 遊戲
    "forum",    # 雲論
]


class EttodayListCrawler(BaseListCrawler):
    """
    List crawler for ETtoday news.

    Fetches article URLs from ETtoday's PC API endpoint.
    """

    @property
    def name(self) -> str:
        return "ettoday_list"

    @property
    def display_name(self) -> str:
        return "ETtoday - List"

    @property
    def source(self) -> str:
        return "ETtoday"

    @property
    def default_interval_minutes(self) -> int:
        return 15

    @property
    def max_pages(self) -> int:
        """Maximum number of pages to crawl."""
        return 10

    def _normalize_article_url(self, href: str) -> str | None:
        """Normalize article URL to full https URL.

        Handles:
        - Relative URLs (/news/YYYYMMDD/ID.htm or /news/ID)
        - Full URLs with any subdomain (https://star.ettoday.net/news/...)
        - Protocol-relative URLs (//www.ettoday.net/news/...)
        - Article paths (/article/ID.htm for game, travel subdomains)

        Returns None if URL doesn't match expected patterns.
        """
        if not href:
            return None

        # Must contain /news/ or /article/ path
        if "/news/" not in href and "/article/" not in href:
            return None

        # Exclude non-article pages
        if "news-list" in href or "hot-news" in href:
            return None

        # Handle protocol-relative URLs
        if href.startswith("//"):
            href = "https:" + href

        # Handle relative URLs - default to www subdomain
        if href.startswith("/"):
            href = f"https://www.ettoday.net{href}"

        # Validate it's an ettoday.net URL with valid subdomain
        subdomain_pattern = "|".join(ETTODAY_SUBDOMAINS)

        # Pattern 1: /news/YYYYMMDD/ID.htm (www subdomain style)
        if re.match(rf"https://({subdomain_pattern})\.ettoday\.net/news/\d{{8}}/\d+\.htm", href):
            return href

        # Pattern 2: /news/ID (subdomain style without .htm, e.g., sports, finance)
        if re.match(rf"https://({subdomain_pattern})\.ettoday\.net/news/\d+$", href):
            return href

        # Pattern 3: /article/ID.htm (game, travel subdomains)
        if re.match(rf"https://({subdomain_pattern})\.ettoday\.net/article/\d+\.htm", href):
            return href

        return None

    def _extract_urls_from_html(self, html: str) -> set[str]:
        """Extract article URLs from HTML content."""
        urls: set[str] = set()
        soup = BeautifulSoup(html, "html.parser")
        links = soup.find_all("a", href=True)

        for link in links:
            url = self._normalize_article_url(link["href"])
            if url:
                urls.add(url)

        return urls

    async def get_article_urls(self) -> list[str]:
        """Fetch article URLs from ETtoday's news list using PC API.

        Strategy:
        - First page: GET https://www.ettoday.net/news/news-list.htm
        - Subsequent pages: POST https://www.ettoday.net/show_roll.php
        """
        all_urls: set[str] = set()
        target_date = datetime.now().strftime("%Y%m%d")

        async with httpx.AsyncClient(timeout=30.0) as client:
            # First page: GET request
            try:
                headers = get_random_headers(
                    referer="https://www.ettoday.net/"
                )
                response = await client.get(
                    "https://www.ettoday.net/news/news-list.htm",
                    headers=headers,
                    follow_redirects=True,
                )
                response.raise_for_status()
                all_urls.update(self._extract_urls_from_html(response.text))
            except httpx.HTTPStatusError as e:
                print(f"[{self.name}] HTTP error on first page: {e}")
                if e.response.status_code == 429:
                    await asyncio.sleep(10)
            except Exception as e:
                print(f"[{self.name}] Error fetching first page: {e}")

            await asyncio.sleep(random.uniform(1, 3))

            # Subsequent pages: POST requests
            post_url = "https://www.ettoday.net/show_roll.php"

            for page in range(2, self.max_pages + 1):
                headers = get_random_headers(
                    referer="https://www.ettoday.net/news/news-list.htm",
                    ajax=True,
                )
                headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"

                # offset is 0-indexed for the API
                payload = {
                    "offset": str(page - 1),
                    "tPage": "3",
                    "tFile": f"{target_date}.xml",
                    "tOt": "0",
                    "tSi": "100",
                    "tAr": "0",
                }

                try:
                    response = await client.post(post_url, headers=headers, data=payload)
                    response.raise_for_status()

                    # Check if we got empty response (no more pages)
                    if not response.text.strip():
                        break

                    all_urls.update(self._extract_urls_from_html(response.text))

                except httpx.HTTPStatusError as e:
                    print(f"[{self.name}] HTTP error on page {page}: {e}")
                    if e.response.status_code == 429:
                        await asyncio.sleep(10)
                        break
                except Exception as e:
                    print(f"[{self.name}] Error fetching page {page}: {e}")

                await asyncio.sleep(random.uniform(1, 3))

        return list(all_urls)

    async def on_success(self, result: CrawlerResult) -> None:
        """Log successful crawl."""
        print(f"[{self.name}] Discovered {result.items_processed} URLs")


class EttodayArticleCrawler(BaseArticleCrawler):
    """
    Article crawler for ETtoday news.

    Fetches and parses individual article pages.
    """

    @property
    def name(self) -> str:
        return "ettoday_article"

    @property
    def display_name(self) -> str:
        return "ETtoday - Article"

    @property
    def source(self) -> str:
        return "ETtoday"

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

    def _get_referer_for_url(self, url: str) -> str:
        """Generate appropriate referer based on article URL subdomain.

        Args:
            url: The article URL (e.g., https://star.ettoday.net/news/...)

        Returns:
            Referer URL matching the article's subdomain.
        """
        for subdomain in ETTODAY_SUBDOMAINS:
            if f"://{subdomain}.ettoday.net/" in url:
                return f"https://{subdomain}.ettoday.net/"
        # Default fallback
        return "https://www.ettoday.net/"

    def _parse_json_ld(self, soup: BeautifulSoup) -> dict:
        """Parse JSON-LD script from page."""
        script_tags = soup.find_all("script", type="application/ld+json")
        for script in script_tags:
            try:
                if script.string:
                    # Remove control characters that may cause JSON parsing to fail
                    json_str = script.string
                    json_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', json_str)
                    data = json.loads(json_str)
                    if isinstance(data, dict) and data.get("@type") == "NewsArticle":
                        return data
            except (json.JSONDecodeError, ValueError):
                continue
        return {}

    def _parse_meta_tags(self, soup: BeautifulSoup) -> dict[str, str]:
        """Parse relevant meta tags from page."""
        meta_mapping = {
            "pubdate": "pubdate",
            "description": "description",
            "section": "section",
            "subsection": "subsection",
            "news_keywords": "news_keywords",
        }
        result = {}
        for meta in soup.find_all("meta"):
            name = meta.get("name", "").lower()
            if name in meta_mapping:
                content = meta.get("content", "")
                if content:
                    result[meta_mapping[name]] = content
            # Also check itemprop for subsection (e.g., itemprop="articleSection")
            itemprop = meta.get("itemprop", "").lower()
            if itemprop == "articlesection" and "subsection" not in result:
                content = meta.get("content", "")
                if content:
                    result["subsection"] = content
        return result

    def _extract_author(self, json_ld: dict) -> str | None:
        """Extract author name from JSON-LD creator field."""
        creator = json_ld.get("creator")
        if not creator:
            return None
        if isinstance(creator, list) and creator:
            # Format: ["1525-陳宛貞", "1525"] - take first and extract name after "-"
            first_creator = creator[0]
            if "-" in first_creator:
                return first_creator.split("-", 1)[1]
            return first_creator
        if isinstance(creator, str):
            if "-" in creator:
                return creator.split("-", 1)[1]
            return creator
        return None

    def _parse_keywords(self, keywords_str: str | None) -> list[str] | None:
        """Parse keywords from '標籤:A,B,C' format."""
        if not keywords_str:
            return None
        # Remove "標籤:" prefix if present
        if keywords_str.startswith("標籤:"):
            keywords_str = keywords_str[3:]
        keywords = [k.strip() for k in keywords_str.split(",") if k.strip()]
        return keywords if keywords else None

    def _normalize_image_url(self, src: str) -> str | None:
        """Normalize image URL to full https URL.

        Supports:
        - cdn.ettoday.net (main CDN)
        - cdn2.ettoday.net, cdn3.ettoday.net (numbered CDNs)
        - static.ettoday.net (static assets)
        - images.ettoday.net (image CDN)
        """
        if not src:
            return None
        # Handle protocol-relative URLs (//cdn2.ettoday.net/...)
        if src.startswith("//"):
            src = "https:" + src
        # Accept ETtoday CDN URLs with various formats
        if re.match(r"https?://(cdn\d*|static|images)\.ettoday\.net/", src):
            return src
        return None

    def _extract_images(self, soup: BeautifulSoup) -> list[str] | None:
        """Extract image URLs from article story div."""
        images = []
        seen = set()

        # Find images in story div
        story_div = soup.find("div", class_="story")
        if story_div:
            for img in story_div.find_all("img", src=True):
                url = self._normalize_image_url(img["src"])
                if url and url not in seen:
                    images.append(url)
                    seen.add(url)

        # Also check for og:image as fallback
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            url = self._normalize_image_url(og_image["content"])
            if url and url not in seen:
                images.append(url)
                seen.add(url)

        return images if images else None

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract article content from story div with image placeholders."""
        story_div = soup.find("div", class_="story")
        if not story_div:
            return ""

        content_parts = []
        # Iterate through direct children to maintain order
        for element in story_div.children:
            if not hasattr(element, "name") or not element.name:
                continue

            if element.name == "p":
                # Check if paragraph contains an image
                img = element.find("img", src=True)
                if img:
                    url = self._normalize_image_url(img["src"])
                    if url:
                        content_parts.append(f"[{url}]")
                # Also get text content
                text = element.get_text(strip=True)
                if text:
                    content_parts.append(text)
            elif element.name == "img":
                # Direct image in story div
                url = self._normalize_image_url(element.get("src", ""))
                if url:
                    content_parts.append(f"[{url}]")

        return "\n\n".join(content_parts)

    def _parse_published_at(self, meta: dict, json_ld: dict) -> datetime | None:
        """Parse published_at from meta or JSON-LD, convert to UTC for storage."""
        from zoneinfo import ZoneInfo

        # Priority: meta pubdate > JSON-LD datePublished
        date_str = meta.get("pubdate") or json_ld.get("datePublished")
        if not date_str:
            return None
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            # Convert to UTC naive datetime for consistent storage with other fields
            if dt.tzinfo is not None:
                dt_utc = dt.astimezone(ZoneInfo("UTC"))
                return dt_utc.replace(tzinfo=None)
            return dt
        except ValueError:
            return None

    def parse_html(self, raw_html: str, url: str) -> ArticleData:
        """Parse article data from raw HTML without making network requests."""
        soup = BeautifulSoup(raw_html, "html.parser")

        # Parse structured data
        json_ld = self._parse_json_ld(soup)
        meta = self._parse_meta_tags(soup)

        # Extract title from <h1>
        title = ""
        h1_tag = soup.find("h1")
        if h1_tag:
            title = h1_tag.get_text(strip=True)

        # Extract fields using helper methods
        author = self._extract_author(json_ld)
        published_at = self._parse_published_at(meta, json_ld)
        category = meta.get("section") or json_ld.get("articleSection")
        sub_category = meta.get("subsection")
        summary = meta.get("description")
        tags = self._parse_keywords(meta.get("news_keywords"))
        content = self._extract_content(soup)
        images = self._extract_images(soup)

        return ArticleData(
            url=url,
            title=title,
            content=content,
            summary=summary,
            author=author,
            category=category,
            sub_category=sub_category,
            tags=tags,
            published_at=published_at,
            images=images,
        )

    async def fetch_article(self, url: str) -> ArticleData:
        """Fetch and parse a single ETtoday article."""
        referer = self._get_referer_for_url(url)

        async with httpx.AsyncClient(timeout=30.0) as client:
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
