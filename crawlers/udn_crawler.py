"""UDN (聯合新聞網) news crawler implementations."""

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


def get_random_headers(referer: str | None = None) -> dict[str, str]:
    """Generate headers with random User-Agent."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    if referer:
        headers["Referer"] = referer
    return headers


class UdnListCrawler(BaseListCrawler):
    """
    List crawler for UDN (聯合新聞網) news.

    Fetches article URLs from UDN's breaknews page and pagination API.
    """

    @property
    def name(self) -> str:
        return "udn_list"

    @property
    def display_name(self) -> str:
        return "UDN - List"

    @property
    def source(self) -> str:
        return "UDN"

    @property
    def default_interval_minutes(self) -> int:
        return 15

    @property
    def max_pages(self) -> int:
        """Maximum number of pages to crawl."""
        return 10

    def _parse_total_rec_no(self, html: str) -> str | None:
        """Parse totalRecNo from page HTML's #indicator data-query attribute."""
        soup = BeautifulSoup(html, "html.parser")
        indicator = soup.find(id="indicator")
        if not indicator:
            return None

        data_query = indicator.get("data-query", "")
        if not data_query:
            return None

        # Parse the data-query which is a Python dict-like string
        # e.g., "{'cate_id': '99','type':'breaknews','totalRecNo':'37396'}"
        match = re.search(r"'totalRecNo'\s*:\s*'(\d+)'", data_query)
        if match:
            return match.group(1)
        return None

    def _extract_urls_from_html(self, soup: BeautifulSoup) -> set[str]:
        """Extract article URLs from HTML page."""
        urls = set()
        # Find all story list items
        for item in soup.select(".story-list__news"):
            link = item.find("a", href=True)
            if link:
                href = link["href"]
                # UDN article URLs pattern: /news/story/XXXX/XXXXXXX
                if "/news/story/" in href:
                    # Remove query parameters and convert to full URL
                    clean_url = href.split("?")[0]
                    if clean_url.startswith("/"):
                        clean_url = f"https://udn.com{clean_url}"
                    urls.add(clean_url)
        return urls

    def _extract_urls_from_api(self, data: dict) -> set[str]:
        """Extract article URLs from API response."""
        urls = set()
        lists = data.get("lists", [])
        for item in lists:
            title_link = item.get("titleLink", "")
            if title_link and "/news/story/" in title_link:
                # Remove query parameters and convert to full URL
                clean_url = title_link.split("?")[0]
                if clean_url.startswith("/"):
                    clean_url = f"https://udn.com{clean_url}"
                urls.add(clean_url)
        return urls

    async def get_article_urls(self) -> list[str]:
        """Fetch article URLs from UDN's breaknews list."""
        all_urls: set[str] = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            # Step 1: Fetch first page HTML to get totalRecNo
            first_page_url = "https://udn.com/news/breaknews/1/99"
            headers = get_random_headers(referer="https://udn.com/")

            try:
                response = await client.get(first_page_url, headers=headers)
                response.raise_for_status()
                html = response.text

                # Extract URLs from first page
                soup = BeautifulSoup(html, "html.parser")
                first_page_urls = self._extract_urls_from_html(soup)
                all_urls.update(first_page_urls)

                # Get totalRecNo for pagination
                total_rec_no = self._parse_total_rec_no(html)
                if not total_rec_no:
                    print(f"[{self.name}] Could not find totalRecNo, using only first page")
                    return list(all_urls)

            except httpx.HTTPStatusError as e:
                print(f"[{self.name}] HTTP error fetching first page: {e}")
                return list(all_urls)
            except Exception as e:
                print(f"[{self.name}] Error fetching first page: {e}")
                return list(all_urls)

            # Random delay before pagination
            await asyncio.sleep(random.uniform(1, 3))

            # Step 2: Fetch additional pages via API
            api_url = "https://udn.com/api/more"

            for page in range(2, self.max_pages + 1):
                headers = get_random_headers(referer="https://udn.com/news/breaknews/1/99")
                headers["Accept"] = "application/json, text/javascript, */*; q=0.01"
                headers["X-Requested-With"] = "XMLHttpRequest"

                params = {
                    "page": str(page),
                    "id": "",
                    "channelId": "1",
                    "cate_id": "99",
                    "type": "breaknews",
                    "totalRecNo": total_rec_no,
                }

                try:
                    response = await client.get(api_url, headers=headers, params=params)
                    response.raise_for_status()

                    data = response.json()

                    # Check if we've reached the end
                    if not data.get("state", False):
                        break

                    # Extract URLs from API response
                    page_urls = self._extract_urls_from_api(data)
                    all_urls.update(page_urls)

                    # Check if this is the last page
                    if data.get("end", False):
                        break

                except httpx.HTTPStatusError as e:
                    print(f"[{self.name}] HTTP error on page {page}: {e}")
                    if e.response.status_code == 429:
                        # Too many requests - back off and stop
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


class UdnArticleCrawler(BaseArticleCrawler):
    """
    Article crawler for UDN (聯合新聞網) news.

    Fetches and parses individual article pages.
    """

    @property
    def name(self) -> str:
        return "udn_article"

    @property
    def display_name(self) -> str:
        return "UDN - Article"

    @property
    def source(self) -> str:
        return "UDN"

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

            # Check og:description as fallback
            prop = meta.get("property", "").lower()
            if prop == "og:description" and "description" not in result:
                content = meta.get("content", "")
                if content:
                    result["description"] = content

        return result

    def _extract_author(self, soup: BeautifulSoup, json_ld: dict) -> str | None:
        """Extract author name from page or JSON-LD."""
        # Try JSON-LD first (most reliable)
        author = json_ld.get("author")
        if author:
            if isinstance(author, dict):
                name = author.get("name")
                if name:
                    return name
            if isinstance(author, str):
                return author

        # Fallback to HTML author span
        author_span = soup.find("span", class_="article-content__author")
        if author_span:
            author_text = author_span.get_text(strip=True)
            # Parse "記者XXX／...報導" format - author is XXX
            match = re.search(r"記者([^／/]+)[／/]", author_text)
            if match:
                return match.group(1).strip()
            # For "聯合新聞網／綜合報導" format - author is after slash
            if "／" in author_text or "/" in author_text:
                parts = re.split(r"[／/]", author_text)
                if len(parts) >= 2:
                    # Return the part after slash (reporter/author name)
                    return parts[1].strip()

        return None

    def _normalize_image_url(self, src: str) -> str | None:
        """Normalize image URL to full https URL."""
        if not src:
            return None
        # Handle protocol-relative URLs
        if src.startswith("//"):
            src = "https:" + src
        # Accept UDN image URLs
        if "pgw.udn.com.tw" in src or "uc.udn.com.tw" in src or "udn.com" in src:
            return src
        return None

    def _extract_images(self, soup: BeautifulSoup) -> list[str] | None:
        """Extract image URLs from article."""
        images = []
        seen = set()

        # Find cover image
        cover = soup.find("figure", class_="article-content__cover")
        if cover:
            img = cover.find("img", src=True)
            if img:
                url = self._normalize_image_url(img["src"])
                if url and url not in seen:
                    images.append(url)
                    seen.add(url)

        # Find images in article content
        editor_section = soup.find("section", class_="article-content__editor")
        if editor_section:
            for img in editor_section.find_all("img", src=True):
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
        """Extract article content from editor section with image placeholders."""
        editor_section = soup.find("section", class_="article-content__editor")
        if not editor_section:
            return ""

        content_parts = []

        # Iterate through paragraphs
        for element in editor_section.find_all(["p", "figure"]):
            if element.name == "p":
                # Check if paragraph contains an image
                img = element.find("img", src=True)
                if img:
                    url = self._normalize_image_url(img["src"])
                    if url:
                        content_parts.append(f"[{url}]")
                # Get text content
                text = element.get_text(strip=True)
                if text:
                    content_parts.append(text)
            elif element.name == "figure":
                # Handle figure elements with images
                img = element.find("img", src=True)
                if img:
                    url = self._normalize_image_url(img["src"])
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
            # Convert to UTC naive datetime for consistent storage
            if dt.tzinfo is not None:
                dt_utc = dt.astimezone(ZoneInfo("UTC"))
                return dt_utc.replace(tzinfo=None)
            return dt
        except ValueError:
            return None

    def _parse_keywords(self, meta: dict, json_ld: dict) -> list[str] | None:
        """Parse keywords from meta tags or JSON-LD."""
        # Try meta news_keywords first
        keywords_str = meta.get("news_keywords")
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

        # Extract title from <h1 class="article-content__title">
        title = ""
        h1_tag = soup.find("h1", class_="article-content__title")
        if h1_tag:
            title = h1_tag.get_text(strip=True)

        # Extract fields using helper methods
        author = self._extract_author(soup, json_ld)
        published_at = self._parse_published_at(meta, json_ld)
        category = meta.get("section")
        sub_category = meta.get("subsection")
        summary = meta.get("description")
        tags = self._parse_keywords(meta, json_ld)
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
        """Fetch and parse a single UDN article."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            headers = get_random_headers(referer="https://udn.com/")
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
