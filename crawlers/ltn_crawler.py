"""自由時報 (Liberty Times Net) 新聞爬蟲實作。"""

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
        "Accept": "*/*" if ajax else "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    if referer:
        headers["Referer"] = referer
    if ajax:
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["sec-fetch-dest"] = "empty"
        headers["sec-fetch-mode"] = "cors"
        headers["sec-fetch-site"] = "same-origin"
    return headers


class LtnListCrawler(BaseListCrawler):
    """
    List crawler for 自由時報 (Liberty Times Net).

    Fetches article URLs from LTN's AJAX API endpoint.
    """

    @property
    def name(self) -> str:
        return "ltn_list"

    @property
    def display_name(self) -> str:
        return "自由時報 - List"

    @property
    def source(self) -> str:
        return "LTN"

    @property
    def default_interval_minutes(self) -> int:
        return 15

    @property
    def max_pages(self) -> int:
        """Maximum number of pages to crawl."""
        return 10

    async def get_article_urls(self) -> list[str]:
        """Fetch article URLs from LTN's breaking news AJAX API."""
        base_url = "https://news.ltn.com.tw/ajax/breakingnews/all"
        all_urls: set[str] = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            for page in range(1, self.max_pages + 1):
                headers = get_random_headers(
                    referer="https://news.ltn.com.tw/list/breakingnews/all",
                    ajax=True,
                )

                try:
                    response = await client.get(f"{base_url}/{page}", headers=headers)
                    response.raise_for_status()

                    data = response.json()
                    if data.get("code") != 200:
                        print(f"[{self.name}] API returned code {data.get('code')} on page {page}")
                        continue

                    # Parse article URLs from response data
                    # API returns a list of article objects
                    articles_data = data.get("data", [])
                    if isinstance(articles_data, list):
                        for article in articles_data:
                            if isinstance(article, dict) and "url" in article:
                                url = article["url"]
                                if url:
                                    all_urls.add(url)
                    elif isinstance(articles_data, dict):
                        # Fallback for dictionary format
                        for article in articles_data.values():
                            if isinstance(article, dict) and "url" in article:
                                url = article["url"]
                                if url:
                                    all_urls.add(url)

                except httpx.HTTPStatusError as e:
                    print(f"[{self.name}] HTTP error on page {page}: {e}")
                    if e.response.status_code == 429:
                        # Too many requests - back off and stop
                        await asyncio.sleep(10)
                        break
                except json.JSONDecodeError as e:
                    print(f"[{self.name}] JSON decode error on page {page}: {e}")
                except Exception as e:
                    print(f"[{self.name}] Error fetching page {page}: {e}")

                # Random delay between pages to avoid being banned
                await asyncio.sleep(random.uniform(1, 3))

        return list(all_urls)

    async def on_success(self, result: CrawlerResult) -> None:
        """Log successful crawl."""
        print(f"[{self.name}] Discovered {result.items_processed} URLs")


class LtnArticleCrawler(BaseArticleCrawler):
    """
    Article crawler for 自由時報 (Liberty Times Net).

    Fetches and parses individual article pages from all LTN subdomains:
    - news.ltn.com.tw (主站)
    - ec.ltn.com.tw (財經)
    - ent.ltn.com.tw (娛樂)
    - sports.ltn.com.tw (體育)
    - health.ltn.com.tw (健康)
    """

    @property
    def name(self) -> str:
        return "ltn_article"

    @property
    def display_name(self) -> str:
        return "自由時報 - Article"

    @property
    def source(self) -> str:
        return "LTN"

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
        result = {}
        for meta in soup.find_all("meta"):
            name = meta.get("name", "").lower()
            prop = meta.get("property", "").lower()
            content = meta.get("content", "")

            if name == "description" and content:
                result["description"] = content
            elif name == "keywords" and content:
                result["keywords"] = content
            elif prop == "article:section" and content:
                result["section"] = content
            elif prop == "article:section2" and content:
                result["section2"] = content
            elif prop == "og:image" and content:
                result["og_image"] = content

        return result

    def _extract_author(self, soup: BeautifulSoup, json_ld: dict) -> str | None:
        """Extract author/reporter name from article."""
        # Try to extract from article_edit span (format: "〔記者XXX／地點報導〕")
        edit_span = soup.find("span", class_="article_edit")
        if edit_span:
            text = edit_span.get_text(strip=True)
            # Pattern: 〔記者XXX／地點報導〕 or 〔記者XXX/地點報導〕
            match = re.search(r'〔記者([^／/]+)[／/]', text)
            if match:
                return match.group(1).strip()

        # Also check content for reporter pattern
        content_div = soup.find("div", class_="text")
        if content_div:
            first_p = content_div.find("p")
            if first_p:
                text = first_p.get_text(strip=True)
                match = re.search(r'〔記者([^／/]+)[／/]', text)
                if match:
                    return match.group(1).strip()

        # Fallback to JSON-LD author
        author = json_ld.get("author")
        if isinstance(author, dict):
            return author.get("name")
        elif isinstance(author, str):
            return author

        return None

    def _parse_keywords(self, keywords_str: str | None, json_ld: dict) -> list[str] | None:
        """Parse keywords from meta tag or JSON-LD."""
        # Try JSON-LD keywords first (usually more structured)
        json_ld_keywords = json_ld.get("keywords")
        if isinstance(json_ld_keywords, list) and json_ld_keywords:
            return [k.strip() for k in json_ld_keywords if k.strip()]

        # Fallback to meta keywords
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
        # Only accept LTN image URLs
        if re.match(r"https?://img\.ltn\.com\.tw/", src):
            return src
        return None

    def _get_image_url(self, img_tag) -> str | None:
        """Get image URL from img tag, handling lazy-loading (data-src)."""
        # Try data-src first (lazy-loaded images)
        src = img_tag.get("data-src") or img_tag.get("src")
        return self._normalize_image_url(src)

    def _extract_images(self, soup: BeautifulSoup, json_ld: dict) -> list[str] | None:
        """Extract image URLs from article."""
        images = []
        seen = set()

        # Extract from JSON-LD first
        json_ld_images = json_ld.get("image", [])
        if isinstance(json_ld_images, list):
            for img in json_ld_images:
                if isinstance(img, dict):
                    url = img.get("contentUrl") or img.get("url")
                elif isinstance(img, str):
                    url = img
                else:
                    continue
                url = self._normalize_image_url(url)
                if url and url not in seen:
                    images.append(url)
                    seen.add(url)

        # Extract from photo div (handles lazy-loading with data-src)
        photo_divs = soup.find_all("div", class_="photo")
        for photo_div in photo_divs:
            for img in photo_div.find_all("img"):
                url = self._get_image_url(img)
                if url and url not in seen:
                    images.append(url)
                    seen.add(url)

        # Extract from content area (handles lazy-loading with data-src)
        content_div = soup.find("div", class_="text")
        if content_div:
            for img in content_div.find_all("img"):
                url = self._get_image_url(img)
                if url and url not in seen:
                    images.append(url)
                    seen.add(url)

        return images if images else None

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract article content from text div with image placeholders."""
        # Find main content div - try multiple selectors for different subdomains
        # Priority: specific data-desc attributes (if they have content) > generic text class with p tags
        content_div = None

        # Try data-desc="內容頁" first (main site)
        candidate = soup.find("div", class_="text", attrs={"data-desc": "內容頁"})
        if candidate and candidate.find_all(["p", "h2", "h3", "h4"]):
            content_div = candidate

        # Try data-desc="內文" (some subdomains)
        if not content_div:
            candidate = soup.find("div", class_="text", attrs={"data-desc": "內文"})
            if candidate and candidate.find_all(["p", "h2", "h3", "h4"]):
                content_div = candidate

        # Fallback: Find div.text that actually contains <p> tags
        if not content_div:
            for div in soup.find_all("div", class_="text"):
                if div.find_all("p"):
                    content_div = div
                    break

        if not content_div:
            return ""

        content_parts = []
        seen_images = set()

        # Process photo divs first (they appear before text)
        for photo_div in soup.find_all("div", class_="photo"):
            img = photo_div.find("img")
            if img:
                url = self._get_image_url(img)
                if url and url not in seen_images:
                    content_parts.append(f"[{url}]")
                    seen_images.add(url)
            # Add photo caption if present
            caption = photo_div.find("p")
            if caption:
                caption_text = caption.get_text(strip=True)
                if caption_text:
                    content_parts.append(caption_text)

        # Process main content - find all text-bearing elements
        for element in content_div.find_all(["p", "h2", "h3", "h4", "img"]):
            # Skip "請繼續往下閱讀" prompts and ads
            element_classes = element.get("class", [])
            if "before_ir" in element_classes:
                continue
            if element.get("id", "").startswith("ad-"):
                continue

            if element.name == "p":
                # Check if paragraph contains an image
                img = element.find("img")
                if img:
                    url = self._get_image_url(img)
                    if url and url not in seen_images:
                        content_parts.append(f"[{url}]")
                        seen_images.add(url)
                # Get text content
                text = element.get_text(strip=True)
                if text:
                    content_parts.append(text)
            elif element.name in ("h2", "h3", "h4"):
                text = element.get_text(strip=True)
                if text:
                    content_parts.append(text)
            elif element.name == "img":
                url = self._get_image_url(element)
                if url and url not in seen_images:
                    content_parts.append(f"[{url}]")
                    seen_images.add(url)

        return "\n\n".join(content_parts)

    def _parse_published_at(self, soup: BeautifulSoup, meta: dict, json_ld: dict) -> datetime | None:
        """Parse published_at from HTML, meta, or JSON-LD, convert to UTC for storage."""
        from zoneinfo import ZoneInfo

        # Priority 1: JSON-LD datePublished
        date_str = json_ld.get("datePublished")

        # Priority 2: HTML article_time span
        if not date_str:
            time_span = soup.find("span", class_="article_time")
            if time_span:
                date_str = time_span.get_text(strip=True)
                # Format: "2026/01/08 13:46" - convert to ISO format
                try:
                    dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
                    # Assume Taiwan timezone (UTC+8)
                    tw_tz = ZoneInfo("Asia/Taipei")
                    dt = dt.replace(tzinfo=tw_tz)
                    dt_utc = dt.astimezone(ZoneInfo("UTC"))
                    return dt_utc.replace(tzinfo=None)
                except ValueError:
                    pass

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

    def _extract_category(self, soup: BeautifulSoup, meta: dict, json_ld: dict) -> str | None:
        """Extract article category."""
        # Try meta tag first
        if meta.get("section"):
            return meta["section"]

        # Try breadcrumbs
        breadcrumbs = soup.find("div", class_="breadcrumbs")
        if breadcrumbs:
            links = breadcrumbs.find_all("a")
            if len(links) >= 2:
                # Usually: 首頁 > 分類 > ...
                return links[-1].get_text(strip=True)

        # Try JSON-LD articleSection
        if json_ld.get("articleSection"):
            return json_ld["articleSection"]

        return None

    def parse_html(self, raw_html: str, url: str) -> ArticleData:
        """Parse article data from raw HTML without making network requests."""
        soup = BeautifulSoup(raw_html, "html.parser")

        # Parse structured data
        json_ld = self._parse_json_ld(soup)
        meta = self._parse_meta_tags(soup)

        # Extract title from <h1> or JSON-LD
        title = ""
        h1_tag = soup.find("h1")
        if h1_tag:
            title = h1_tag.get_text(strip=True)
        elif json_ld.get("headline"):
            title = json_ld["headline"]

        # Extract fields using helper methods
        author = self._extract_author(soup, json_ld)
        published_at = self._parse_published_at(soup, meta, json_ld)
        category = self._extract_category(soup, meta, json_ld)
        sub_category = meta.get("section2")
        summary = meta.get("description") or json_ld.get("description")
        tags = self._parse_keywords(meta.get("keywords"), json_ld)
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
        """Fetch and parse a single LTN article."""
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            headers = get_random_headers(referer="https://news.ltn.com.tw/")
            response = await client.get(url, headers=headers)

            # Handle 429 Too Many Requests with backoff
            if response.status_code == 429:
                await asyncio.sleep(10)
                response = await client.get(url, headers=headers)

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
