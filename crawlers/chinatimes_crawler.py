"""China Times (中國時報) news crawler implementations."""

import asyncio
import json
import random
import re
from datetime import datetime
from zoneinfo import ZoneInfo

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
    """Generate headers with random User-Agent."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if referer:
        headers["Referer"] = referer
    return headers


class ChinaTimesListCrawler(BaseListCrawler):
    """
    List crawler for China Times (中國時報) news.

    Fetches article URLs from China Times' realtimenews page with pagination.
    """

    BASE_URL = "https://www.chinatimes.com"

    @property
    def name(self) -> str:
        return "chinatimes_list"

    @property
    def display_name(self) -> str:
        return "中國時報 - 列表"

    @property
    def source(self) -> str:
        return "China Times"

    @property
    def default_interval_minutes(self) -> int:
        return 15

    @property
    def max_pages(self) -> int:
        """Maximum number of pages to crawl."""
        return 10

    def _extract_urls_from_html(self, soup: BeautifulSoup) -> set[str]:
        """Extract article URLs from HTML page."""
        urls = set()
        # Find all article items in the vertical list
        vertical_list = soup.find("ul", class_="vertical-list")
        if not vertical_list:
            return urls

        for item in vertical_list.find_all("li"):
            # Find title link
            title_elem = item.find("h3", class_="title")
            if title_elem:
                link = title_elem.find("a", href=True)
                if link:
                    href = link["href"]
                    # China Times article URLs pattern: /realtimenews/YYYYMMDDXXXXXX-XXXXXX
                    if "/realtimenews/" in href:
                        # Remove query parameters and convert to full URL
                        clean_url = href.split("?")[0]
                        if clean_url.startswith("/"):
                            clean_url = f"{self.BASE_URL}{clean_url}"
                        urls.add(clean_url)
        return urls

    async def get_article_urls(self) -> list[str]:
        """Fetch article URLs from China Times' realtimenews list."""
        all_urls: set[str] = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            for page in range(1, self.max_pages + 1):
                page_url = f"{self.BASE_URL}/realtimenews/?page={page}&chdtv"
                headers = get_random_headers(referer=f"{self.BASE_URL}/realtimenews/")

                try:
                    response = await client.get(page_url, headers=headers)
                    response.raise_for_status()
                    html = response.text

                    # Extract URLs from page
                    soup = BeautifulSoup(html, "html.parser")
                    page_urls = self._extract_urls_from_html(soup)
                    all_urls.update(page_urls)

                    # If no URLs found, likely reached the end
                    if not page_urls:
                        print(f"[{self.name}] No URLs found on page {page}, stopping")
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
                if page < self.max_pages:
                    await asyncio.sleep(random.uniform(1, 3))

        return list(all_urls)

    async def on_success(self, result: CrawlerResult) -> None:
        """Log successful crawl."""
        print(f"[{self.name}] Discovered {result.items_processed} URLs")


class ChinaTimesArticleCrawler(BaseArticleCrawler):
    """
    Article crawler for China Times (中國時報) news.

    Fetches and parses individual article pages.
    """

    BASE_URL = "https://www.chinatimes.com"

    @property
    def name(self) -> str:
        return "chinatimes_article"

    @property
    def display_name(self) -> str:
        return "中國時報 - 文章"

    @property
    def source(self) -> str:
        return "China Times"

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
        meta_mapping = {
            "pubdate": "pubdate",
            "description": "description",
            "section": "section",
            "subsection": "subsection",
            "keywords": "keywords",
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

    def _extract_title(self, soup: BeautifulSoup, json_ld: dict) -> str:
        """Extract article title."""
        # Try JSON-LD headline first
        headline = json_ld.get("headline")
        if headline:
            return headline

        # Try h1.article-title
        h1_tag = soup.find("h1", class_="article-title")
        if h1_tag:
            return h1_tag.get_text(strip=True)

        # Fallback to og:title
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            return og_title["content"]

        return ""

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

        # Fallback to HTML author div
        author_div = soup.find("div", class_="author")
        if author_div:
            author_link = author_div.find("a")
            if author_link:
                return author_link.get_text(strip=True)
            # Try text directly
            author_text = author_div.get_text(strip=True)
            if author_text:
                return author_text

        return None

    def _normalize_image_url(self, src: str) -> str | None:
        """Normalize image URL to full https URL."""
        if not src:
            return None
        # Handle protocol-relative URLs
        if src.startswith("//"):
            src = "https:" + src
        # Accept China Times image URLs
        if "chinatimes.com" in src or "images.chinatimes.com" in src:
            return src
        return None

    def _extract_images(self, soup: BeautifulSoup, json_ld: dict) -> list[str] | None:
        """Extract image URLs from article."""
        images = []
        seen = set()

        # Try JSON-LD image first
        json_image = json_ld.get("image")
        if json_image:
            if isinstance(json_image, dict):
                url = json_image.get("url")
                if url and url not in seen:
                    images.append(url)
                    seen.add(url)
            elif isinstance(json_image, str):
                if json_image not in seen:
                    images.append(json_image)
                    seen.add(json_image)

        # Find main figure image
        main_figure = soup.find("div", class_="main-figure")
        if main_figure:
            figure = main_figure.find("figure")
            if figure:
                img = figure.find("img", src=True)
                if img:
                    url = self._normalize_image_url(img["src"])
                    if url and url not in seen:
                        images.append(url)
                        seen.add(url)

        # Find images in article content
        article_body = soup.find("div", class_="article-body")
        if article_body:
            for figure in article_body.find_all("figure"):
                img = figure.find("img", src=True)
                if img:
                    url = self._normalize_image_url(img["src"])
                    if url and url not in seen:
                        images.append(url)
                        seen.add(url)

        # Also check for og:image as fallback
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            url = og_image["content"]
            if url and url not in seen:
                images.append(url)
                seen.add(url)

        return images if images else None

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract article content from article-body section with image placeholders."""
        article_body = soup.find("div", class_="article-body")
        if not article_body:
            return ""

        content_parts = []

        # Iterate through paragraphs
        for element in article_body.find_all(["p", "figure"]):
            if element.name == "p":
                # Skip promote-word ads
                if element.find_parent("div", class_="promote-word"):
                    continue
                # Skip ad containers
                if element.find_parent("div", class_="ad"):
                    continue
                # Skip donate form
                if element.find_parent("div", id="donate-form-container"):
                    continue

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

    def _extract_category(self, soup: BeautifulSoup, meta: dict) -> tuple[str | None, str | None]:
        """Extract category and sub-category."""
        category = None
        sub_category = None

        # Try meta section first
        category = meta.get("section")
        sub_category = meta.get("subsection")

        # Fallback to HTML category div
        if not category:
            category_div = soup.find("div", class_="category")
            if category_div:
                category_link = category_div.find("a")
                if category_link:
                    category = category_link.get_text(strip=True)

        return category, sub_category

    def _extract_tags(self, soup: BeautifulSoup, json_ld: dict, meta: dict) -> list[str] | None:
        """Extract tags/keywords from page."""
        # Try JSON-LD keywords first
        keywords = json_ld.get("keywords")
        if keywords:
            if isinstance(keywords, str):
                tags = [k.strip() for k in keywords.split(",") if k.strip()]
                if tags:
                    return tags
            elif isinstance(keywords, list):
                if keywords:
                    return keywords

        # Try meta keywords
        keywords_str = meta.get("keywords")
        if keywords_str:
            tags = [k.strip() for k in keywords_str.split(",") if k.strip()]
            if tags:
                return tags

        # Fallback to HTML hash tags
        hash_tag_div = soup.find("div", class_="article-hash-tag")
        if hash_tag_div:
            tags = []
            for span in hash_tag_div.find_all("span", class_="hash-tag"):
                tag_link = span.find("a")
                if tag_link:
                    tag_text = tag_link.get_text(strip=True)
                    if tag_text:
                        tags.append(tag_text)
            if tags:
                return tags

        return None

    def _parse_published_at(self, soup: BeautifulSoup, json_ld: dict, meta: dict) -> datetime | None:
        """Parse published_at from JSON-LD, meta, or HTML, convert to UTC for storage."""
        # Priority: JSON-LD datePublished > meta pubdate > HTML time element
        date_str = json_ld.get("datePublished") or meta.get("pubdate")

        if not date_str:
            # Try HTML time element
            time_elem = soup.find("time", datetime=True)
            if time_elem:
                date_str = time_elem.get("datetime")

        if not date_str:
            return None

        try:
            # Handle various date formats
            if "T" in date_str:
                # ISO 8601 format
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            else:
                # Fallback: try parsing "YYYY-MM-DD HH:MM" format
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
                    # Assume Taiwan timezone
                    dt = dt.replace(tzinfo=ZoneInfo("Asia/Taipei"))
                except ValueError:
                    return None

            # Convert to UTC naive datetime for consistent storage
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

        # Extract fields using helper methods
        title = self._extract_title(soup, json_ld)
        author = self._extract_author(soup, json_ld)
        published_at = self._parse_published_at(soup, json_ld, meta)
        category, sub_category = self._extract_category(soup, meta)
        summary = meta.get("description")
        tags = self._extract_tags(soup, json_ld, meta)
        content = self._extract_content(soup)
        images = self._extract_images(soup, json_ld)

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
        """Fetch and parse a single China Times article."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            headers = get_random_headers(referer=f"{self.BASE_URL}/realtimenews/")
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
