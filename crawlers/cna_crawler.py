"""CNA (中央社) news crawler implementations."""

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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
]


def get_random_headers(referer: str | None = None) -> dict[str, str]:
    """Generate headers with random User-Agent for CNA requests."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.cna.com.tw",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def get_article_headers(referer: str | None = None) -> dict[str, str]:
    """Generate headers with random User-Agent for article page requests."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    if referer:
        headers["Referer"] = referer
    return headers


class CnaListCrawler(BaseListCrawler):
    """
    List crawler for CNA (中央社) news.

    Fetches article URLs from CNA's JSON API endpoint.
    The API returns paginated news list with 100 items per page.
    """

    @property
    def name(self) -> str:
        return "cna_list"

    @property
    def display_name(self) -> str:
        return "中央社 - List"

    @property
    def source(self) -> str:
        return "CNA"

    @property
    def default_interval_minutes(self) -> int:
        return 15

    @property
    def max_pages(self) -> int:
        """Maximum number of pages to crawl per execution."""
        return 5

    @property
    def page_size(self) -> int:
        """Number of items per page from API."""
        return 100

    async def _fetch_page(
        self, client: httpx.AsyncClient, page_idx: int
    ) -> list[str]:
        """
        Fetch a single page of article URLs from CNA API.

        Args:
            client: HTTP client instance
            page_idx: Page index (1-based)

        Returns:
            List of article URLs from this page
        """
        url = "https://www.cna.com.tw/cna2018api/api/WNewsList"
        headers = get_random_headers(referer="https://www.cna.com.tw/list/aall.aspx")
        headers["Content-Type"] = "application/json"

        payload = {
            "action": "0",
            "category": "aall",  # All categories - 即時新聞
            "pagesize": str(self.page_size),
            "pageidx": page_idx,
        }

        response = await client.post(url, headers=headers, json=payload)
        response.raise_for_status()

        data = response.json()
        urls: list[str] = []

        # Check if response is successful
        if data.get("Result") != "Y":
            print(f"[{self.name}] API returned error on page {page_idx}")
            return urls

        # Extract URLs from ResultData.Items
        result_data = data.get("ResultData", {})
        items = result_data.get("Items", [])

        for item in items:
            page_url = item.get("PageUrl", "")
            if page_url:
                # Ensure URL is absolute
                if page_url.startswith("/"):
                    page_url = f"https://www.cna.com.tw{page_url}"
                urls.append(page_url)

        return urls

    async def get_article_urls(self) -> list[str]:
        """Fetch article URLs from CNA's news list API for multiple pages."""
        all_urls: set[str] = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            for page in range(1, self.max_pages + 1):
                try:
                    urls = await self._fetch_page(client, page)
                    all_urls.update(urls)

                    print(f"[{self.name}] Page {page}: found {len(urls)} URLs")

                    # If we got fewer items than page_size, we've reached the end
                    if len(urls) < self.page_size:
                        break

                except httpx.HTTPStatusError as e:
                    print(f"[{self.name}] HTTP error on page {page}: {e}")
                    if e.response.status_code == 429:
                        # Too many requests - back off and stop pagination
                        print(f"[{self.name}] Rate limited, stopping pagination")
                        await asyncio.sleep(10)
                        break
                except Exception as e:
                    print(f"[{self.name}] Error fetching page {page}: {e}")

                # Random delay between pages to avoid being banned
                await asyncio.sleep(random.uniform(1, 3))

        return list(all_urls)

    async def on_success(self, result: CrawlerResult) -> None:
        """Log successful crawl."""
        print(f"[{self.name}] Discovered {result.items_processed} URLs")


class CnaArticleCrawler(BaseArticleCrawler):
    """
    Article crawler for CNA (中央社) news.

    Fetches and parses individual article pages.
    CNA articles contain JSON-LD structured data which makes parsing reliable.
    """

    @property
    def name(self) -> str:
        return "cna_article"

    @property
    def display_name(self) -> str:
        return "中央社 - Article"

    @property
    def source(self) -> str:
        return "CNA"

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
        """
        Parse JSON-LD script from page.

        CNA pages contain structured data with @type: "NewsArticle"
        which includes articleBody, datePublished, etc.
        """
        script_tags = soup.find_all("script", type="application/ld+json")
        for script in script_tags:
            try:
                if script.string:
                    # Remove control characters that may cause JSON parsing to fail
                    json_str = script.string
                    json_str = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", json_str)
                    data = json.loads(json_str)

                    # Handle array of JSON-LD objects
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

        # Check og:description for summary
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            result["description"] = og_desc["content"]

        # Check standard meta description as fallback
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content") and "description" not in result:
            result["description"] = meta_desc["content"]

        # Check author meta tag
        author_meta = soup.find("meta", property="author")
        if author_meta and author_meta.get("content"):
            result["author"] = author_meta["content"]

        return result

    def _extract_reporter_from_content(self, content: str) -> str | None:
        """
        Extract reporter name from CNA byline patterns in content.

        Patterns:
        - 中央社記者戴雅真東京19日專電 → reporter is "戴雅真"
        - 中央社記者沈如峰宜蘭縣20日電 → reporter is "沈如峰"
        - 中央社台北20日電 → no reporter (just location)
        - 中央社XX日綜合外電報導 → no reporter (foreign news compilation)

        Returns:
            Reporter name or None if not found/applicable.
        """
        if not content:
            return None

        # Pattern: 中央社記者<name><location><date>電/專電
        # Reporter names are typically 2-3 Chinese characters
        # Location follows, ending with common suffixes before the date
        # Common location endings: 市, 縣, 區 or just city names like 台北, 東京
        # Date pattern: XX日

        # Try 3-character name first (most common)
        pattern_3char = r"中央社記者([\u4e00-\u9fff]{3})[\u4e00-\u9fff]+\d{1,2}日[專]?電"
        match = re.search(pattern_3char, content)
        if match:
            return match.group(1)

        # Try 2-character name
        pattern_2char = r"中央社記者([\u4e00-\u9fff]{2})[\u4e00-\u9fff]+\d{1,2}日[專]?電"
        match = re.search(pattern_2char, content)
        if match:
            return match.group(1)

        return None

    def _extract_author(self, content: str, meta: dict) -> str | None:
        """
        Extract author name.

        Priority:
        1. Reporter name from content byline pattern (e.g., 中央社記者XXX)
        2. Meta tag author field (fallback for 綜合外電 etc.)
        """
        # First try to extract reporter from content
        reporter = self._extract_reporter_from_content(content)
        if reporter:
            return reporter

        # Fallback to meta tag author (e.g., "中央通訊社")
        return meta.get("author")

    def _extract_keywords(self, json_ld: dict) -> list[str] | None:
        """Extract keywords from JSON-LD."""
        keywords = json_ld.get("keywords")
        if not keywords:
            return None

        if isinstance(keywords, list):
            return [k for k in keywords if k]
        if isinstance(keywords, str):
            # Keywords might be comma-separated
            return [k.strip() for k in keywords.split(",") if k.strip()]
        return None

    def _normalize_image_url(self, src: str) -> str | None:
        """Normalize image URL to full https URL."""
        if not src:
            return None

        # Handle protocol-relative URLs
        if src.startswith("//"):
            src = "https:" + src

        # Accept CNA CDN URLs and YouTube thumbnails (used for video articles)
        if re.match(r"https?://imgcdn\.cna\.com\.tw/", src):
            return src
        if re.match(r"https?://i\.ytimg\.com/", src):
            return src

        return None

    def _extract_images(self, soup: BeautifulSoup, json_ld: dict) -> list[str] | None:
        """Extract image URLs from article."""
        images = []
        seen = set()

        # Try JSON-LD images first
        json_ld_images = json_ld.get("image", [])
        if isinstance(json_ld_images, list):
            for img_data in json_ld_images:
                if isinstance(img_data, dict):
                    url = img_data.get("url", "")
                elif isinstance(img_data, str):
                    url = img_data
                else:
                    continue

                normalized = self._normalize_image_url(url)
                if normalized and normalized not in seen:
                    images.append(normalized)
                    seen.add(normalized)

        # Find images in paragraph divs
        paragraphs = soup.find_all("div", class_="paragraph")
        for para in paragraphs:
            for img in para.find_all("img", src=True):
                url = self._normalize_image_url(img["src"])
                if url and url not in seen:
                    images.append(url)
                    seen.add(url)

        # Also check og:image as fallback
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            url = self._normalize_image_url(og_image["content"])
            if url and url not in seen:
                images.append(url)
                seen.add(url)

        return images if images else None

    def _extract_content(self, soup: BeautifulSoup, json_ld: dict) -> str:
        """
        Extract article content with image position markers.

        CNA HTML structure:
        - div.centralContent contains the article
        - div.fullPic > figure.floatImg > picture > img for article images
        - div.paragraph > p for text paragraphs

        We traverse centralContent children in order to maintain
        image positions relative to text.
        """
        content_parts = []

        # Find the main content container
        central_content = soup.find("div", class_="centralContent")
        if not central_content:
            # Fallback to JSON-LD articleBody
            article_body = json_ld.get("articleBody", "")
            return article_body.strip() if article_body else ""

        # Process children of centralContent in document order
        for element in central_content.children:
            if not hasattr(element, "name") or not element.name:
                continue

            # Handle image containers (div.fullPic with figure.floatImg)
            if element.name == "div" and "fullPic" in element.get("class", []):
                figure = element.find("figure", class_="floatImg")
                if figure:
                    img = figure.find("img", src=True)
                    if img:
                        img_url = self._normalize_image_url(img["src"])
                        if img_url:
                            content_parts.append(f"[{img_url}]")

            # Handle paragraph containers
            elif element.name == "div" and "paragraph" in element.get("class", []):
                for child in element.children:
                    if not hasattr(child, "name") or not child.name:
                        continue

                    if child.name == "p":
                        text = child.get_text(strip=True)
                        if text:
                            content_parts.append(text)

                    elif child.name == "figure":
                        # Inline figure within paragraphs
                        img = child.find("img", src=True)
                        if img:
                            img_url = self._normalize_image_url(img["src"])
                            if img_url:
                                content_parts.append(f"[{img_url}]")

            # Handle standalone figures
            elif element.name == "figure":
                img = element.find("img", src=True)
                if img:
                    img_url = self._normalize_image_url(img["src"])
                    if img_url:
                        content_parts.append(f"[{img_url}]")

        # If we got content from HTML, return it
        if content_parts:
            return "\n\n".join(content_parts)

        # Fallback: JSON-LD articleBody (no image positions available)
        article_body = json_ld.get("articleBody", "")
        return article_body.strip() if article_body else ""

    def _parse_published_at(self, json_ld: dict) -> datetime | None:
        """Parse published_at from JSON-LD, convert to UTC naive datetime for storage."""
        from zoneinfo import ZoneInfo

        date_str = json_ld.get("datePublished")
        if not date_str:
            return None

        try:
            # CNA uses ISO format with timezone: "2026-01-20T18:04:00+08:00"
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))

            # Convert to UTC naive datetime for consistent storage
            if dt.tzinfo is not None:
                dt_utc = dt.astimezone(ZoneInfo("UTC"))
                return dt_utc.replace(tzinfo=None)
            return dt
        except ValueError:
            return None

    def _extract_category(self, soup: BeautifulSoup, json_ld: dict) -> str | None:
        """Extract category/section from article."""
        # Try JSON-LD articleSection
        section = json_ld.get("articleSection")
        if section:
            return section

        # Try breadcrumb
        breadcrumb = soup.find("div", class_="breadcrumb")
        if breadcrumb:
            links = breadcrumb.find_all("a")
            if len(links) >= 2:
                return links[-1].get_text(strip=True)

        return None

    def parse_html(self, raw_html: str, url: str) -> ArticleData:
        """Parse article data from raw HTML without making network requests."""
        soup = BeautifulSoup(raw_html, "html.parser")

        # Parse structured data
        json_ld = self._parse_json_ld(soup)
        meta = self._parse_meta_tags(soup)

        # Extract title - prioritize JSON-LD, fallback to <h1>
        title = json_ld.get("headline", "")
        if not title:
            h1_tag = soup.find("h1")
            if h1_tag:
                title = h1_tag.get_text(strip=True)

        # Extract fields - content first so we can parse reporter name from it
        content = self._extract_content(soup, json_ld)
        author = self._extract_author(content, meta)
        published_at = self._parse_published_at(json_ld)
        category = self._extract_category(soup, json_ld)
        summary = meta.get("description") or json_ld.get("about", "")
        tags = self._extract_keywords(json_ld)
        images = self._extract_images(soup, json_ld)

        return ArticleData(
            url=url,
            title=title,
            content=content,
            summary=summary,
            author=author,
            category=category,
            sub_category=None,  # CNA doesn't have clear sub-categories
            tags=tags,
            published_at=published_at,
            images=images,
        )

    async def fetch_article(self, url: str) -> ArticleData:
        """Fetch and parse a single CNA article."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            headers = get_article_headers(referer="https://www.cna.com.tw/list/aall.aspx")
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
