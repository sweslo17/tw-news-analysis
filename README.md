# News Analyze

新聞爬蟲管理與分析平台，提供 Web 儀表板即時監控爬蟲排程，以及多階段 Pipeline 篩選新聞文章。

## 功能特色

### 爬蟲管理 (Web Dashboard)
- **爬蟲管理**：註冊、啟用/停用、設定爬蟲參數
- **動態排程**：基於 APScheduler 的任務管理，支援執行期間動態調整
- **即時儀表板**：採用 HTMX 技術的使用者介面，自動更新狀態
- **文章去重**：爬取前進行 URL 去重，避免重複抓取
- **爬蟲隔離**：各爬蟲獨立執行，防止連鎖失敗
- **資料管理**：支援 Raw HTML 封存、還原與重新解析

### 新聞篩選 Pipeline (CLI)
- **規則篩選**：基於關鍵字、分類等規則快速過濾低價值文章
- **Pipeline 管理**：建立、執行、暫停、重置 Pipeline 運行
- **強制納入**：手動指定特定文章強制通過篩選
- **統計報表**：查看篩選效率與歷史執行紀錄

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

- **後端框架**：FastAPI (async)
- **資料庫**：SQLite + SQLAlchemy
- **排程器**：APScheduler
- **前端**：Jinja2 + TailwindCSS (CDN) + HTMX
- **CLI**：Typer + Rich
- **LLM 整合**：OpenAI Batch API
- **套件管理**：Poetry

## 專案結構

```
news-analyze/
├── app/
│   ├── main.py                  # FastAPI 應用程式入口
│   ├── config.py                # Pydantic Settings 設定檔
│   ├── database.py              # 資料庫連線設定
│   ├── models.py                # SQLAlchemy ORM 模型
│   ├── schemas.py               # Pydantic 資料結構
│   ├── scheduler.py             # APScheduler 排程管理
│   ├── services/                # 業務邏輯層
│   │   ├── crawler_service.py
│   │   ├── pending_url_service.py
│   │   ├── data_management_service.py
│   │   ├── deduplication_service.py
│   │   ├── reparse_service.py
│   │   ├── archive_scheduler.py
│   │   └── pipeline/            # 新聞篩選 Pipeline
│   │       ├── pipeline_orchestrator.py  # Pipeline 編排器
│   │       ├── article_fetcher.py        # 文章擷取
│   │       ├── rule_filter_service.py    # 規則篩選
│   │       ├── llm_analysis_service.py   # LLM 分析
│   │       ├── result_store_service.py   # 結果儲存
│   │       └── statistics_service.py     # 統計服務
│   └── templates/               # Jinja2 HTML 模板
├── crawlers/
│   ├── base.py                  # 爬蟲抽象基底類別
│   ├── registry.py              # 爬蟲註冊機制
│   └── *_crawler.py             # 各新聞來源爬蟲實作
├── cli/
│   ├── __main__.py              # CLI 進入點
│   └── pipeline.py              # Pipeline CLI 指令
├── tests/
├── pyproject.toml               # Poetry 專案設定
└── README.md
```

## 快速開始

### 前置需求

- Python 3.11+
- Poetry

### 安裝

```bash
git clone <repository-url>
cd news-analyze

poetry install
```

### 環境變數

複製 `.env.example` 建立 `.env` 檔案，填入需要的 API Key：

```bash
cp .env.example .env
```

```env
# OpenAI API Key (LLM 分析必須)
OPENAI_API_KEY=
```

### 啟動 Web 儀表板

```bash
poetry run uvicorn app.main:app --reload
```

開啟瀏覽器前往 http://localhost:8000

### Pipeline CLI 使用

```bash
# 快速執行：篩選最近 1 天的文章（規則篩選）
poetry run python -m cli quick

# 指定天數
poetry run python -m cli quick --days 3

# 指定小時
poetry run python -m cli quick --hours 2

# 處理昨天的文章
poetry run python -m cli quick --yesterday

# 處理特定日期
poetry run python -m cli quick --date 2025-01-20

# 查看 Pipeline 統計
poetry run python -m cli stats

# 查看特定 Run 的結果
poetry run python -m cli review <run-id> --show-passed --show-filtered

# 匯出結果為 JSON
poetry run python -m cli review <run-id> --export results.json
```

## Pipeline 架構

Pipeline 採用多階段篩選流程：

```
文章資料庫 → [Fetch] → [Rule Filter] → [LLM Analysis] → [Store]
```

| 階段 | 說明 |
|------|------|
| **Fetch** | 依日期範圍從資料庫取出文章 |
| **Rule Filter** | 基於預設規則（關鍵字、分類）快速過濾 |
| **LLM Analysis** | OpenAI Batch API 結構化分析 |
| **Store** | 儲存最終結果與統計 |

## Web API 端點

| 端點 | 方法 | 說明 |
|------|------|------|
| `/` | GET | 主儀表板頁面 |
| `/data-management` | GET | 資料管理頁面 |
| `/health` | GET | 健康檢查 |
| `/api/crawlers/{id}/toggle` | PATCH | 切換爬蟲啟用狀態 |
| `/api/crawlers/{id}/interval` | PATCH | 更新爬蟲執行間隔 |
| `/api/crawlers/{id}/run` | POST | 立即執行爬蟲 |

## 新增爬蟲

1. 在 `crawlers/` 目錄下建立新檔案
2. 繼承 `BaseListCrawler` 或 `BaseArticleCrawler` 類別
3. 實作必要的抽象方法

### 列表爬蟲範例

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
        ...
```

### 文章爬蟲範例

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
        """從 HTML 解析文章內容"""
        ...

    async def fetch_article(self, url: str) -> ArticleData:
        """抓取並解析單篇文章"""
        ...
```

## 開發

### 執行測試

```bash
poetry run pytest
```

### 設計原則

- **SOLID 原則**：爬蟲與 LLM Provider 採用抽象基底類別設計
- **DRY 原則**：共用邏輯抽取至 Service 層
- **分層架構**：明確區分 Model、Service、Router 各層職責

## License

MIT
