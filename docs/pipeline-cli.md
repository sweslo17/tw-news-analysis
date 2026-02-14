# Pipeline CLI 使用指南

新聞文章篩選 Pipeline 的命令列工具使用說明。

## 安裝

確保已安裝所有依賴：

```bash
poetry install
```

## 指令總覽

```bash
poetry run python -m cli.pipeline --help
```

| 指令 | 說明 |
|------|------|
| `quick` | 快速執行 pipeline（建立 + 執行） |
| `create` | 建立新的 pipeline run |
| `run` | 執行指定的 pipeline run |
| `review` | 檢視篩選結果 |
| `stats` | 顯示統計資訊 |
| `reset` | 重置 pipeline run 從指定階段重跑 |
| `force-include` | 將文章加入強制納入清單 |
| `list-force-includes` | 列出所有強制納入的文章 |
| `remove-force-include` | 從強制納入清單移除文章 |
| `providers` | 列出可用的 LLM providers |

---

## quick - 快速執行

最常用的指令，一次完成建立和執行 pipeline。

### 基本用法

```bash
# 預設：最近 24 小時，執行到 rule_filter 階段
poetry run python -m cli.pipeline quick
```

### 時間範圍選項（擇一使用）

```bash
# 最近 N 天
poetry run python -m cli.pipeline quick --days 3
poetry run python -m cli.pipeline quick -d 3

# 最近 N 小時
poetry run python -m cli.pipeline quick --hours 2
poetry run python -m cli.pipeline quick -H 2

# 最近 N 分鐘
poetry run python -m cli.pipeline quick --minutes 60
poetry run python -m cli.pipeline quick -m 60

# 昨天整天 (00:00 ~ 23:59)
poetry run python -m cli.pipeline quick --yesterday
poetry run python -m cli.pipeline quick -y

# 指定日期
poetry run python -m cli.pipeline quick --date 2025-01-20
```

### 執行階段選項

```bash
# 只執行到 rule_filter（預設）
poetry run python -m cli.pipeline quick --until rule_filter

# 執行到 llm_filter（需要 LLM API key）
poetry run python -m cli.pipeline quick --until llm_filter

# 執行完整 pipeline
poetry run python -m cli.pipeline quick --until store
```

### 組合範例

```bash
# 分析昨天的新聞，執行完整 pipeline
poetry run python -m cli.pipeline quick --yesterday --until store

# 分析最近 2 小時，只做 rule filter
poetry run python -m cli.pipeline quick --hours 2 --until rule_filter

# 定期排程：每 30 分鐘分析最近 60 分鐘的新聞
poetry run python -m cli.pipeline quick --minutes 60 --until store
```

---

## create - 建立 Pipeline Run

手動建立 pipeline run，之後再用 `run` 指令執行。

```bash
# 建立指定名稱的 run
poetry run python -m cli.pipeline create --name "2025-01 分析"

# 指定日期範圍
poetry run python -m cli.pipeline create \
  --name "一月第一週" \
  --date-from 2025-01-01 \
  --date-to 2025-01-07
```

### 參數

| 參數 | 縮寫 | 必填 | 說明 |
|------|------|------|------|
| `--name` | `-n` | 是 | Pipeline run 名稱 |
| `--date-from` | | 否 | 開始日期 (YYYY-MM-DD) |
| `--date-to` | | 否 | 結束日期 (YYYY-MM-DD) |

---

## run - 執行 Pipeline Run

執行已建立的 pipeline run。

```bash
# 執行到 rule_filter 階段
poetry run python -m cli.pipeline run 1 --until rule_filter

# 執行到 llm_filter，使用 groq
poetry run python -m cli.pipeline run 1 --until llm_filter --llm-provider groq

# 執行到 llm_filter，指定模型
poetry run python -m cli.pipeline run 1 \
  --until llm_filter \
  --llm-provider anthropic \
  --llm-model claude-3-haiku-20240307

# 執行完整 pipeline
poetry run python -m cli.pipeline run 1 --until store
```

### 參數

| 參數 | 縮寫 | 說明 |
|------|------|------|
| `RUN_ID` | | Pipeline run ID（必填） |
| `--until` | `-u` | 執行到指定階段 |
| `--llm-provider` | `-p` | LLM provider (groq/anthropic/openai/google) |
| `--llm-model` | `-m` | LLM 模型名稱 |

### Pipeline 階段

| 階段 | 說明 |
|------|------|
| `fetch` | 從 DB 取得文章 |
| `rule_filter` | Rule-based 篩選（本機執行） |
| `llm_filter` | LLM 篩選（需要 API key） |
| `llm_analysis` | LLM 分析（框架，尚未實作） |
| `store` | 儲存結果並完成 |

---

## review - 檢視結果

檢視 pipeline run 的篩選結果。

```bash
# 檢視統計摘要
poetry run python -m cli.pipeline review 1

# 顯示被篩選掉的文章
poetry run python -m cli.pipeline review 1 --show-filtered

# 顯示通過篩選的文章
poetry run python -m cli.pipeline review 1 --show-passed

# 限制顯示數量
poetry run python -m cli.pipeline review 1 --show-filtered --limit 50

# 匯出結果到 JSON
poetry run python -m cli.pipeline review 1 --show-filtered --show-passed --export results.json
```

### 參數

| 參數 | 縮寫 | 說明 |
|------|------|------|
| `RUN_ID` | | Pipeline run ID（必填） |
| `--show-filtered` | `-f` | 顯示被篩選掉的文章 |
| `--show-passed` | `-p` | 顯示通過篩選的文章 |
| `--limit` | `-l` | 最大顯示數量（預設 20） |
| `--export` | `-e` | 匯出到 JSON 檔案 |

---

## stats - 統計資訊

顯示 pipeline 統計資訊。

```bash
# 顯示整體統計
poetry run python -m cli.pipeline stats

# 顯示特定 run 的統計
poetry run python -m cli.pipeline stats 1
```

### 輸出內容

- 總執行次數 / 完成次數
- 總處理文章數
- Rule filter / LLM filter 篩選數量
- 平均篩選率
- 最近的 runs 列表
- 各篩選規則的統計

---

## reset - 重置 Pipeline Run

重置 pipeline run，從指定階段重新執行。

```bash
# 從 rule_filter 階段重新開始
poetry run python -m cli.pipeline reset 1 --from-stage rule_filter

# 從 llm_filter 階段重新開始
poetry run python -m cli.pipeline reset 1 --from-stage llm_filter
```

### 參數

| 參數 | 縮寫 | 說明 |
|------|------|------|
| `RUN_ID` | | Pipeline run ID（必填） |
| `--from-stage` | `-s` | 重置起始階段（預設 rule_filter） |

---

## force-include - 強制納入文章

將被誤刪的文章加入強制納入清單，未來的 pipeline run 會自動保留這些文章。

```bash
# 強制納入文章
poetry run python -m cli.pipeline force-include \
  --article-id 12345 \
  --reason "有政治意涵"

# 指定添加者
poetry run python -m cli.pipeline force-include \
  --article-id 12345 \
  --reason "重要新聞" \
  --user "roger"
```

### 參數

| 參數 | 縮寫 | 必填 | 說明 |
|------|------|------|------|
| `--article-id` | `-a` | 是 | 文章 ID |
| `--reason` | `-r` | 是 | 強制納入原因 |
| `--user` | `-u` | 否 | 添加者名稱 |

---

## list-force-includes - 列出強制納入清單

```bash
poetry run python -m cli.pipeline list-force-includes
```

---

## remove-force-include - 移除強制納入

```bash
poetry run python -m cli.pipeline remove-force-include --article-id 12345
```

---

## providers - 列出 LLM Providers

```bash
poetry run python -m cli.pipeline providers
```

### 可用 Providers

| Provider | 預設模型 | 說明 |
|----------|----------|------|
| `groq` | llama-3.1-8b-instant | 速度最快，免費額度高（預設） |
| `anthropic` | claude-3-haiku-20240307 | Claude 系列 |
| `openai` | gpt-4o-mini | GPT 系列 |
| `google` | gemini-1.5-flash | Gemini 系列 |

---

## 環境變數設定

在 `.env` 檔案中設定 API keys：

```env
# LLM API Keys（根據使用的 provider 設定）
GROQ_API_KEY=your-groq-api-key
ANTHROPIC_API_KEY=your-anthropic-api-key
OPENAI_API_KEY=your-openai-api-key
GOOGLE_API_KEY=your-google-api-key

# 預設設定
DEFAULT_LLM_PROVIDER=groq
LLM_FILTER_MODEL=llama-3.1-8b-instant
```

---

## 定期排程設定

### 使用 Cron（Mac/Linux）

```bash
# 編輯 crontab
crontab -e

# 每 30 分鐘執行一次，分析最近 60 分鐘的新聞
*/30 * * * * cd /path/to/news-analyze && poetry run python -m cli.pipeline quick --minutes 60 --until store >> /var/log/pipeline.log 2>&1

# 每天凌晨 2 點執行昨天的完整分析
0 2 * * * cd /path/to/news-analyze && poetry run python -m cli.pipeline quick --yesterday --until store >> /var/log/pipeline-daily.log 2>&1
```

### 查看排程

```bash
crontab -l
```

---

## 常見工作流程

### 1. 日常分析

```bash
# 每天早上分析昨天的新聞
poetry run python -m cli.pipeline quick --yesterday --until rule_filter

# 檢視被篩選的文章，確認沒有誤刪
poetry run python -m cli.pipeline review <run_id> --show-filtered

# 如果有誤刪，加入強制納入清單
poetry run python -m cli.pipeline force-include -a <article_id> -r "原因"

# 重置並重跑
poetry run python -m cli.pipeline reset <run_id> --from-stage rule_filter
poetry run python -m cli.pipeline run <run_id> --until store
```

### 2. 即時監控

```bash
# 每 30 分鐘執行一次
poetry run python -m cli.pipeline quick --minutes 60 --until store

# 查看最新統計
poetry run python -m cli.pipeline stats
```

### 3. 歷史資料分析

```bash
# 建立指定日期範圍的 run
poetry run python -m cli.pipeline create \
  --name "2025年1月分析" \
  --date-from 2025-01-01 \
  --date-to 2025-01-31

# 執行完整 pipeline
poetry run python -m cli.pipeline run <run_id> --until store

# 匯出結果
poetry run python -m cli.pipeline review <run_id> --show-passed --export january-2025.json
```

---

## 預設篩選規則

| 規則名稱 | 類型 | 篩選內容 |
|----------|------|----------|
| `horoscope_filter` | keyword | 星座運勢、塔羅牌、占卜 |
| `lottery_filter` | pattern | 威力彩/大樂透開獎、彩券號碼 |
| `ad_filter` | keyword | [廣告]、廣編特輯、業配文 |
| `weather_routine_filter` | pattern | 例行天氣預報（非極端天氣） |

這些規則會自動套用，高精度只篩除確定不重要的文章。
