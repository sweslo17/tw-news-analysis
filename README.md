# 新聞爬蟲管理平台

一個基於 Web 的新聞爬蟲管理平台，提供即時監控與動態排程功能。

## 功能特色

- **爬蟲管理**：註冊、啟用/停用、設定爬蟲參數
- **動態排程**：基於 APScheduler 的任務管理，支援執行期間動態調整
- **即時儀表板**：採用 HTMX 技術的使用者介面，自動更新狀態
- **文章去重**：爬取前進行 URL 去重，避免重複抓取
- **爬蟲隔離**：各爬蟲獨立執行，防止連鎖失敗
- **資料管理**：支援 Raw HTML 封存、還原與重新解析

## 已支援的新聞來源

| 來源 | 爬蟲名稱 | 類型 |
|------|---------|------|
| ETtoday 新聞雲 | `ettoday` | 列表 + 文章 |
| 聯合新聞網 | `udn` | 列表 + 文章 |
| 自由時報 | `ltn` | 列表 + 文章 |
| 中時新聞網 | `chinatimes` | 列表 + 文章 |
| 三立新聞網 | `setn` | 列表 + 文章 |
| 中央通訊社 | `cna` | 列表 + 文章 |
| TVBS 新聞 | `tvbs` | 列表 + 文章 |

## 技術架構

- **後端框架**：FastAPI（非同步）
- **資料庫**：SQLite + SQLAlchemy
- **排程器**：APScheduler
- **前端**：Jinja2 + TailwindCSS (CDN) + HTMX

## 專案結構

```
news-analyze/
├── app/
│   ├── main.py              # FastAPI 應用程式入口
│   ├── config.py            # 設定檔
│   ├── database.py          # 資料庫連線設定
│   ├── models.py            # SQLAlchemy ORM 模型
│   ├── schemas.py           # Pydantic 資料結構
│   ├── scheduler.py         # APScheduler 排程管理
│   ├── services/            # 業務邏輯層
│   │   ├── crawler_service.py
│   │   ├── pending_url_service.py
│   │   ├── data_management_service.py
│   │   ├── deduplication_service.py
│   │   ├── reparse_service.py
│   │   └── archive_scheduler.py
│   └── templates/           # Jinja2 HTML 模板
├── crawlers/
│   ├── base.py              # 爬蟲抽象基底類別
│   ├── registry.py          # 爬蟲註冊機制
│   └── *_crawler.py         # 各新聞來源爬蟲實作
├── tests/                   # 測試檔案
├── pyproject.toml           # Poetry 專案設定
└── README.md
```

## 快速開始

### 前置需求

- Python 3.11+
- Poetry

### 安裝步驟

```bash
# 複製專案
git clone <repository-url>
cd news-analyze

# 安裝依賴套件
poetry install

# 啟動應用程式
poetry run uvicorn app.main:app --reload

# 開啟瀏覽器
open http://localhost:8000
```

### API 端點

| 端點 | 方法 | 說明 |
|------|------|------|
| `/` | GET | 主儀表板頁面 |
| `/data-management` | GET | 資料管理頁面 |
| `/health` | GET | 健康檢查 |
| `/api/crawlers/{id}/toggle` | PATCH | 切換爬蟲啟用狀態 |
| `/api/crawlers/{id}/interval` | PATCH | 更新爬蟲執行間隔 |
| `/api/crawlers/{id}/run` | POST | 立即執行爬蟲 |

## 新增爬蟲

### 步驟

1. 在 `crawlers/` 目錄下建立新檔案
2. 繼承 `BaseListCrawler` 或 `BaseArticleCrawler` 類別
3. 實作必要的抽象方法

### 列表爬蟲範例

列表爬蟲負責從 RSS、Sitemap 或索引頁面取得文章 URL 列表。

```python
from crawlers.base import BaseListCrawler

class MyNewsListCrawler(BaseListCrawler):
    @property
    def name(self) -> str:
        return "my_news_list"

    @property
    def display_name(self) -> str:
        return "My News 列表爬蟲"

    @property
    def source(self) -> str:
        return "My News"

    async def get_article_urls(self) -> list[str]:
        """從 RSS 或網頁取得文章 URL 列表"""
        # 實作取得 URL 列表的邏輯
        ...
```

### 文章爬蟲範例

文章爬蟲負責抓取並解析文章內容。

```python
from crawlers.base import BaseArticleCrawler, ArticleData

class MyNewsArticleCrawler(BaseArticleCrawler):
    @property
    def name(self) -> str:
        return "my_news_article"

    @property
    def display_name(self) -> str:
        return "My News 文章爬蟲"

    @property
    def source(self) -> str:
        return "My News"

    def parse_html(self, raw_html: str, url: str) -> ArticleData:
        """從 HTML 解析文章內容（用於重新解析）"""
        # 實作 HTML 解析邏輯
        ...

    async def fetch_article(self, url: str) -> ArticleData:
        """抓取並解析單篇文章"""
        # 實作抓取與解析邏輯
        ...
```

### ArticleData 資料結構

```python
@dataclass
class ArticleData:
    url: str                          # 文章網址（必填）
    title: str                        # 標題（必填）
    content: str                      # 內文（必填）
    summary: str | None = None        # 摘要
    author: str | None = None         # 作者
    category: str | None = None       # 分類
    sub_category: str | None = None   # 子分類
    tags: list[str] | None = None     # 標籤
    published_at: datetime | None = None  # 發布時間
    raw_html: str | None = None       # 原始 HTML
    images: list[str] | None = None   # 圖片網址列表
```

## 資料管理

### 統計功能

查看各新聞來源的文章數量與儲存空間使用情況。

### 封存功能

將舊文章的 Raw HTML 封存至檔案系統，減少資料庫空間佔用。

- 支援依日期篩選（指定天數前或全部）
- 支援單一來源或所有來源批次封存
- 封存後可還原

### 重新解析

使用最新的解析邏輯重新解析已封存的文章，更新文章內容。

## 開發指南

### 執行測試

```bash
poetry run pytest
```

### 程式碼規範

本專案遵循以下原則：

- **SOLID 原則**：爬蟲採用抽象基底類別設計，遵循開放封閉原則
- **DRY 原則**：共用邏輯抽取至 Service 層
- **分層架構**：明確區分 Model、Service、Router 各層職責

## 授權

MIT License
