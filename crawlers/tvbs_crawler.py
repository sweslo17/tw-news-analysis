"""TVBS news crawler implementations."""

import asyncio
import json
import random
import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from crawlers.base import ArticleData, BaseArticleCrawler, BaseListCrawler

# User-Agent list for rotation to avoid being banned
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

TVBS_BASE_URL = "https://news.tvbs.com.tw"


def get_random_headers(referer: str | None = None) -> dict[str, str]:
    """Generate headers with random User-Agent.

    Args:
        referer: Optional referer URL.
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    if referer:
        headers["Referer"] = referer
    return headers


class TvbsListCrawler(BaseListCrawler):
    """
    List crawler for TVBS news.

    Fetches article URLs from TVBS's realtime news page.
    """

    @property
    def name(self) -> str:
        return "tvbs_list"

    @property
    def display_name(self) -> str:
        return "TVBS News List Crawler"

    @property
    def source(self) -> str:
        return "TVBS"

    @property
    def default_interval_minutes(self) -> int:
        return 15

    def _normalize_article_url(self, href: str) -> str | None:
        """Normalize article URL to full https URL.

        Args:
            href: The href attribute from anchor tag.

        Returns:
            Full URL or None if not a valid article URL.
        """
        if not href:
            return None

        # Handle relative URLs
        if href.startswith("/"):
            href = f"{TVBS_BASE_URL}{href}"

        # Validate TVBS article URL pattern (e.g., https://news.tvbs.com.tw/xxxxx/1234567)
        if re.match(r"https://news\.tvbs\.com\.tw/\w+/\d+", href):
            return href

        return None

    def _extract_urls_from_html(self, html: str) -> set[str]:
        """Extract article URLs from realtime news page HTML.

        Parses the news list container and extracts URLs from list items.
        Filters out ads and items without valid headlines.
        """
        urls: set[str] = set()
        soup = BeautifulSoup(html, "html.parser")

        # Find the news list container: div.news_list > div.list > ul > li
        news_list = soup.find("div", class_="news_list")
        if not news_list:
            return urls

        list_div = news_list.find("div", class_="list")
        if not list_div:
            return urls

        ul = list_div.find("ul")
        if not ul:
            return urls

        for li in ul.find_all("li"):
            # Filter out ads: skip if class="adsbox"
            if li.get("class") and "adsbox" in li.get("class"):
                continue

            # Filter out ads: skip if id contains "news_m_index_list"
            li_id = li.get("id", "")
            if li_id and "news_m_index_list" in li_id:
                continue

            # Filter out items without h2 headline
            h2 = li.find("h2", class_="txt")
            if not h2:
                continue

            # Extract URL from anchor tag
            anchor = li.find("a", href=True)
            if anchor:
                url = self._normalize_article_url(anchor["href"])
                if url:
                    urls.add(url)

        return urls

    async def get_article_urls(self) -> list[str]:
        """Fetch article URLs from TVBS realtime news page."""
        all_urls: set[str] = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                headers = get_random_headers(referer=TVBS_BASE_URL)
                response = await client.get(
                    f"{TVBS_BASE_URL}/realtime",
                    headers=headers,
                    follow_redirects=True,
                )
                response.raise_for_status()
                all_urls.update(self._extract_urls_from_html(response.text))

            except httpx.HTTPStatusError as e:
                print(f"[{self.name}] HTTP error: {e}")
                if e.response.status_code == 429:
                    await asyncio.sleep(10)
            except Exception as e:
                print(f"[{self.name}] Error fetching realtime page: {e}")

        return list(all_urls)


class TvbsArticleCrawler(BaseArticleCrawler):
    """
    Article crawler for TVBS news.

    Fetches and parses individual article pages using:
    1. JSON-LD structured data
    2. Meta tags (og:title, og:description, etc.)
    3. HTML content (article.article_content)
    """

    @property
    def name(self) -> str:
        return "tvbs_article"

    @property
    def display_name(self) -> str:
        return "TVBS News Article Crawler"

    @property
    def source(self) -> str:
        return "TVBS"

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
        """Parse JSON-LD script from page.

        Looks for NewsArticle type in JSON-LD structured data.
        """
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
        """Parse relevant meta tags from page.

        Extracts og:*, article:*, and other relevant meta tags.
        """
        result = {}

        # Open Graph tags
        og_mapping = {
            "og:title": "title",
            "og:description": "description",
            "og:image": "image",
        }
        for og_prop, key in og_mapping.items():
            meta = soup.find("meta", property=og_prop)
            if meta and meta.get("content"):
                result[key] = meta["content"]

        # Article tags
        article_mapping = {
            "article:published_time": "published_time",
            "article:section": "section",
        }
        for article_prop, key in article_mapping.items():
            meta = soup.find("meta", property=article_prop)
            if meta and meta.get("content"):
                result[key] = meta["content"]

        # Name-based meta tags
        name_mapping = {
            "author": "author",
            "keywords": "keywords",
            "description": "meta_description",
        }
        for name, key in name_mapping.items():
            meta = soup.find("meta", attrs={"name": name})
            if meta and meta.get("content"):
                result[key] = meta["content"]

        return result

    def _parse_keywords(self, keywords_str: str | None) -> list[str] | None:
        """Parse keywords string into list."""
        if not keywords_str:
            return None
        keywords = [k.strip() for k in keywords_str.split(",") if k.strip()]
        return keywords if keywords else None

    def _normalize_image_url(self, src: str) -> str | None:
        """Normalize image URL to full https URL."""
        if not src:
            return None

        # Handle protocol-relative URLs
        if src.startswith("//"):
            src = "https:" + src

        # Handle relative URLs
        if src.startswith("/"):
            src = f"{TVBS_BASE_URL}{src}"

        # Accept TVBS image URLs
        if "tvbs.com.tw" in src or "tvbs.tw" in src:
            return src

        return None

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract article content from article.article_content."""
        article = soup.find("article", class_="article_content")
        if not article:
            # Fallback: try to find div with article content
            article = soup.find("div", class_="article_content")
        if not article:
            return ""

        content_parts = []

        # Extract text from paragraphs
        for p in article.find_all("p"):
            text = p.get_text(strip=True)
            if text:
                content_parts.append(text)

        return "\n\n".join(content_parts)

    def _extract_images(self, soup: BeautifulSoup, meta: dict) -> list[str] | None:
        """Extract image URLs from article content and meta tags."""
        images = []
        seen = set()

        # First add og:image if available
        og_image = meta.get("image")
        if og_image:
            url = self._normalize_image_url(og_image)
            if url and url not in seen:
                images.append(url)
                seen.add(url)

        # Extract images from article content
        article = soup.find("article", class_="article_content")
        if not article:
            article = soup.find("div", class_="article_content")

        if article:
            # Look for images with data-original (lazy loaded)
            for img in article.find_all("img"):
                src = img.get("data-original") or img.get("src")
                if src:
                    url = self._normalize_image_url(src)
                    if url and url not in seen:
                        images.append(url)
                        seen.add(url)

        return images if images else None

    def _parse_published_at(self, meta: dict, json_ld: dict) -> datetime | None:
        """Parse published_at from meta or JSON-LD, convert to UTC for storage."""
        from zoneinfo import ZoneInfo

        # Priority: meta article:published_time > JSON-LD datePublished
        date_str = meta.get("published_time") or json_ld.get("datePublished")
        if not date_str:
            return None

        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            # Convert to UTC naive datetime for consistent storage
            if dt.tzinfo is not None:
                dt_utc = dt.astimezone(ZoneInfo("UTC"))
                return dt_utc.replace(tzinfo=None)
            return dt
        except ValueError:
            return None

    def _extract_author(self, meta: dict, json_ld: dict) -> str | None:
        """Extract author from meta tags or JSON-LD."""
        # Try meta tag first
        if meta.get("author"):
            return meta["author"]

        # Try JSON-LD author
        author = json_ld.get("author")
        if author:
            if isinstance(author, dict):
                return author.get("name")
            if isinstance(author, str):
                return author

        return None

    def parse_html(self, raw_html: str, url: str) -> ArticleData:
        """Parse article data from raw HTML without making network requests."""
        soup = BeautifulSoup(raw_html, "html.parser")

        # Parse structured data
        json_ld = self._parse_json_ld(soup)
        meta = self._parse_meta_tags(soup)

        # Extract title: og:title > JSON-LD headline > h1
        title = meta.get("title") or json_ld.get("headline") or ""
        if not title:
            h1_tag = soup.find("h1")
            if h1_tag:
                title = h1_tag.get_text(strip=True)

        # Extract other fields
        author = self._extract_author(meta, json_ld)
        published_at = self._parse_published_at(meta, json_ld)
        category = meta.get("section") or json_ld.get("articleSection")
        summary = meta.get("description") or meta.get("meta_description")
        tags = self._parse_keywords(meta.get("keywords") or json_ld.get("keywords"))
        content = self._extract_content(soup)
        images = self._extract_images(soup, meta)

        return ArticleData(
            url=url,
            title=title,
            content=content,
            summary=summary,
            author=author,
            category=category,
            tags=tags,
            published_at=published_at,
            images=images,
        )

    async def fetch_article(self, url: str) -> ArticleData:
        """Fetch and parse a single TVBS article."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            headers = get_random_headers(referer=f"{TVBS_BASE_URL}/realtime")
            response = await client.get(url, headers=headers, follow_redirects=True)

            # Handle 429 Too Many Requests with backoff
            if response.status_code == 429:
                await asyncio.sleep(10)
                headers = get_random_headers(referer=f"{TVBS_BASE_URL}/realtime")
                response = await client.get(url, headers=headers, follow_redirects=True)

            response.raise_for_status()
            raw_html = response.text

        # Parse the HTML using the shared method
        article = self.parse_html(raw_html, url)
        article.raw_html = raw_html

        # Add random delay between articles to avoid being banned
        await asyncio.sleep(random.uniform(1, 3))

        return article
