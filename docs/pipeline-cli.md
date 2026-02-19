# Pipeline CLI 使用指南

新聞文章篩選與分析 Pipeline 的命令列工具使用說明。

## 安裝

```bash
poetry install
```

## 指令總覽

```bash
poetry run python -m cli.pipeline --help
```

### 主指令

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

### analysis 子指令

| 指令 | 說明 |
|------|------|
| `analysis status` | 顯示 LLM 分析追蹤統計 |
| `analysis retry-failed` | 重新送出 LLM 分析失敗的文章（重跑 LLM） |
| `analysis retry-storage` | 重試 TimescaleDB 寫入失敗的文章（不重跑 LLM） |
| `analysis clear` | 清除分析追蹤記錄（含 TimescaleDB） |

### Pipeline 階段

```
fetch → rule_filter → llm_analysis → store
```

| 階段 | 說明 |
|------|------|
| `fetch` | 從 SQLite 取得文章 |
| `rule_filter` | Rule-based 篩選（本機執行，不需 API） |
| `llm_analysis` | OpenAI Batch API 分析（需要 `OPENAI_API_KEY`） |
| `store` | 儲存結果到 TimescaleDB 並完成 |

---

## 情境指南

### 1. 測試用：少量文章跑完整 Pipeline

用 `--latest --limit` 抓最新的 N 篇文章，適合首次測試或 debug。

```bash
# 最新 10 篇文章，跑到 LLM 分析
poetry run python -m cli.pipeline quick --latest --limit 10 --until llm_analysis

# 最新 10 篇文章，跑完整 pipeline（含 TimescaleDB 寫入）
poetry run python -m cli.pipeline quick --latest --limit 10 --until store

# 也可以搭配時間範圍，取最近 7 天中的 10 篇
poetry run python -m cli.pipeline quick --days 7 --limit 10 --until store

# 取昨天的新聞，只處理 5 篇測試
poetry run python -m cli.pipeline quick --yesterday --limit 5 --until store
```

### 2. 日常分析：每天跑一次

```bash
# Step 1: 分析昨天的新聞，先只跑 rule filter 確認篩選結果
poetry run python -m cli.pipeline quick --yesterday

# Step 2: 檢視被篩選掉的文章，確認沒有誤刪
poetry run python -m cli.pipeline review <RUN_ID> --show-filtered

# Step 3: 確認 OK 後，繼續跑 LLM 分析 + 存入 TimescaleDB
poetry run python -m cli.pipeline run <RUN_ID> --until store

# 或者一步到位（直接跑完整 pipeline）
poetry run python -m cli.pipeline quick --yesterday --until store
```

### 3. 即時監控：定期自動執行

```bash
# 每次分析最近 60 分鐘的新聞，跑完整 pipeline
poetry run python -m cli.pipeline quick --minutes 60 --until store

# 每次分析最近 2 小時的新聞
poetry run python -m cli.pipeline quick --hours 2 --until store

# 查看最新統計
poetry run python -m cli.pipeline stats
```

### 4. 歷史資料回補

```bash
# 建立指定日期範圍的 run
poetry run python -m cli.pipeline create \
  --name "2025年1月分析" \
  --date-from 2025-01-01 \
  --date-to 2025-01-31

# 執行完整 pipeline
poetry run python -m cli.pipeline run <RUN_ID> --until store

# 匯出結果
poetry run python -m cli.pipeline review <RUN_ID> --show-passed --export january-2025.json
```

### 5. LLM 分析失敗：重試

分析失敗有兩種狀態，需要不同的重試方式：

| 狀態 | 原因 | 重試指令 | 說明 |
|------|------|----------|------|
| `FAILED` | LLM 回傳格式錯誤、enum 不合法等 | `retry-failed` | 需要重新呼叫 LLM |
| `STORE_FAILED` | TimescaleDB 連線失敗等暫時性錯誤 | `retry-storage` | 不需重新呼叫 LLM |

```bash
# 先查看各狀態數量
poetry run python -m cli.pipeline analysis status

# 重試 LLM 分析失敗的文章（重新送 OpenAI Batch API）
poetry run python -m cli.pipeline analysis retry-failed

# 重試 TimescaleDB 寫入失敗的文章（不重跑 LLM，直接重試寫入）
poetry run python -m cli.pipeline analysis retry-storage
```

### 6. 清除分析記錄

清除 tracking 記錄時，已成功寫入 TimescaleDB 的資料也會一併刪除。

```bash
# 清除所有追蹤記錄（含 TimescaleDB 資料）
poetry run python -m cli.pipeline analysis clear --all

# 只清除失敗的記錄（TimescaleDB 不受影響，因為失敗的文章本來就沒寫入）
poetry run python -m cli.pipeline analysis clear --failed

# 清除指定文章的記錄
poetry run python -m cli.pipeline analysis clear --article-id 12345

# 清除指定 batch 的記錄
poetry run python -m cli.pipeline analysis clear --batch-id batch_abc123
```

### 7. 篩選結果有誤：強制納入文章

當 rule filter 誤刪重要文章時，可以加入強制納入清單。

```bash
# 先檢視被篩選掉的文章
poetry run python -m cli.pipeline review <RUN_ID> --show-filtered

# 將誤刪的文章加入強制納入清單
poetry run python -m cli.pipeline force-include \
  --article-id 12345 \
  --reason "有政治意涵" \
  --user "roger"

# 查看強制納入清單
poetry run python -m cli.pipeline list-force-includes

# 重置 pipeline 從 rule_filter 重跑
poetry run python -m cli.pipeline reset <RUN_ID> --from-stage rule_filter
poetry run python -m cli.pipeline run <RUN_ID> --until store

# 如果不需要了，移除
poetry run python -m cli.pipeline remove-force-include --article-id 12345
```

### 8. Pipeline 中斷：恢復執行

如果 pipeline 在 LLM 分析階段中斷（例如 polling 超時），狀態會變成 `PAUSED`，可以直接恢復。

```bash
# 查看 run 狀態
poetry run python -m cli.pipeline stats <RUN_ID>

# 直接重新執行（LLM batch 有 resume 機制，不會重送已提交的 batch）
poetry run python -m cli.pipeline run <RUN_ID> --until store
```

### 9. 重新執行某個階段

```bash
# 從 rule_filter 重跑（會刪除 rule_filter 之後的所有結果）
poetry run python -m cli.pipeline reset <RUN_ID> --from-stage rule_filter
poetry run python -m cli.pipeline run <RUN_ID> --until store

# 從 llm_analysis 重跑
poetry run python -m cli.pipeline reset <RUN_ID> --from-stage llm_analysis
poetry run python -m cli.pipeline run <RUN_ID> --until store
```

---

## 指令參數詳解

### quick

| 參數 | 縮寫 | 預設 | 說明 |
|------|------|------|------|
| `--days` | `-d` | 1 | 最近 N 天（預設，無指定時間範圍時自動使用） |
| `--hours` | `-H` | - | 最近 N 小時 |
| `--minutes` | `-m` | - | 最近 N 分鐘 |
| `--yesterday` | `-y` | - | 昨天整天 (00:00~23:59) |
| `--date` | - | - | 指定日期 (YYYY-MM-DD) |
| `--latest` | - | - | 不限日期，取最新文章（搭配 `--limit` 使用） |
| `--until` | `-u` | `rule_filter` | 執行到指定階段 |
| `--limit` | `-l` | 無限制 | 最大處理文章數 |

> 時間範圍選項（`--days`、`--hours`、`--minutes`、`--yesterday`、`--date`、`--latest`）只能擇一使用。

### create

| 參數 | 縮寫 | 必填 | 說明 |
|------|------|------|------|
| `--name` | `-n` | 是 | Pipeline run 名稱 |
| `--date-from` | - | 否 | 開始日期 (YYYY-MM-DD) |
| `--date-to` | - | 否 | 結束日期 (YYYY-MM-DD) |

### run

| 參數 | 縮寫 | 說明 |
|------|------|------|
| `RUN_ID` | - | Pipeline run ID（必填） |
| `--until` | `-u` | 執行到指定階段（預設 `store`） |

### review

| 參數 | 縮寫 | 說明 |
|------|------|------|
| `RUN_ID` | - | Pipeline run ID（必填） |
| `--show-filtered` | `-f` | 顯示被篩選掉的文章 |
| `--show-passed` | `-p` | 顯示通過篩選的文章 |
| `--limit` | `-l` | 最大顯示數量（預設 20） |
| `--export` | `-e` | 匯出到 JSON 檔案 |

### stats

| 參數 | 說明 |
|------|------|
| `RUN_ID` | Pipeline run ID（選填，不填則顯示整體統計） |

### reset

| 參數 | 縮寫 | 說明 |
|------|------|------|
| `RUN_ID` | - | Pipeline run ID（必填） |
| `--from-stage` | `-s` | 重置起始階段（預設 `rule_filter`） |

### force-include

| 參數 | 縮寫 | 必填 | 說明 |
|------|------|------|------|
| `--article-id` | `-a` | 是 | 文章 ID |
| `--reason` | `-r` | 是 | 強制納入原因 |
| `--user` | `-u` | 否 | 添加者名稱 |

### analysis status

無參數。

### analysis retry-failed

無參數。自動重新送出所有 `FAILED` 狀態的文章。

### analysis retry-storage

無參數。自動重試所有 `STORE_FAILED` 狀態的文章寫入 TimescaleDB。

### analysis clear

| 參數 | 縮寫 | 說明 |
|------|------|------|
| `--all` | - | 清除所有追蹤記錄（含 TimescaleDB） |
| `--failed` | - | 只清除失敗的記錄 |
| `--article-id` | `-a` | 清除指定文章的記錄 |
| `--batch-id` | `-b` | 清除指定 batch 的記錄 |

> 以上四個選項只能擇一使用。

---

## 環境變數

在 `.env` 檔案中設定：

```env
# OpenAI（LLM 分析必須）
OPENAI_API_KEY=your-openai-api-key

# TimescaleDB（分析結果儲存，不設定則不寫入）
TIMESCALE_URL=postgres://user:pass@host:port/dbname?sslmode=require

# 其他 LLM providers（rule filter 用，選填）
GROQ_API_KEY=your-groq-api-key
ANTHROPIC_API_KEY=your-anthropic-api-key
GOOGLE_API_KEY=your-google-api-key

# 預設設定
DEFAULT_LLM_PROVIDER=groq
LLM_FILTER_MODEL=llama-3.1-8b-instant
```

---

## Cron 排程範例

```bash
crontab -e
```

```cron
# 每 30 分鐘：分析最近 60 分鐘的新聞，跑完整 pipeline
*/30 * * * * cd /path/to/tw-news-analysis && poetry run python -m cli.pipeline quick --minutes 60 --until store >> /var/log/pipeline.log 2>&1

# 每天凌晨 2 點：分析昨天的新聞
0 2 * * * cd /path/to/tw-news-analysis && poetry run python -m cli.pipeline quick --yesterday --until store >> /var/log/pipeline-daily.log 2>&1
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
