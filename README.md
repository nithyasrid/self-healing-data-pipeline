## Snowflake-Native Autonomous Data Reliability Platform - A Self-Healing Data Pipeline

An autonomous, production-grade data pipeline built entirely on **Snowflake** that monitors data quality, detects anomalies, classifies root causes, and applies automated remediation — without human intervention.

"Have I invented a new technology?"

"No. Data observability and self-healing pipelines already exist in industry. My goal was to understand how those systems work by designing and implementing a Snowflake-native version from scratch."

![Snowflake](https://img.shields.io/badge/Platform-Snowflake-29B5E8?style=flat&logo=snowflake)
![Status](https://img.shields.io/badge/Status-Production_Ready-brightgreen)
---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SELF-HEALING PIPELINE                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  [Data Sources] ──→ [Landing Tables] ──→ [Quality Checks]               │
│   API / S3 /           RAW Schema         5 automated checks             │
│   Kafka / SFTP                                    │                      │
│                                                   ▼                      │
│                                        [Anomaly Stream]                  │
│                                                   │                      │
│                                                   ▼                      │
│                                        [AI Classifier]                   │
│                                        Root cause + severity             │
│                                                   │                      │
│                                    ┌──────────────┴──────────┐           │
│                                    ▼                         ▼           │
│                           [Auto-Remediate]          [Escalate/Alert]     │
│                           Fix NULLs, dedup,         Human approval       │
│                           quarantine                queue                 │
│                                    │                                     │
│                                    ▼                                     │
│                           [Audit Log + Dashboard]                        │
│                           Cost tracking, patterns                         │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```
<img width="1536" height="1024" alt="ChatGPT Image Jul 19, 2026, 03_16_19 PM" src="https://github.com/user-attachments/assets/491d7e4f-df73-4912-aa24-7941b912302d" />
---

## Features

### Core Pipeline
- **5 Automated Quality Checks**: NULL rate, volume, duplicates, schema drift, range validation
- **Rule-Based Classifier**: Determines root cause and recommended action for each failure
- **Auto-Remediation Engine**: Fixes NULLs (recomputes from other columns), removes duplicates, quarantines outliers
- **Task Graph Orchestration**: 3-task DAG running every 5 minutes (Snowflake-native scheduling)
- **Full Audit Trail**: Every action logged with timestamp, actor, and success status

### Advanced Enhancements
- **Anomaly Pattern Learning**: Tracks recurring issues, predicts when next failure will occur
- **Cross-Table Dependency Awareness**: Auto-pauses downstream tables when upstream fails
- **Cost & ROI Tracking**: Calculates engineer-hours saved, downtime prevented, total $ saved
- **Human-in-the-Loop Approval**: Low-confidence fixes go to review queue (approve/reject)
- **Multi-Source Ingestion Monitor**: Tracks health of APIs, S3, Kafka, SFTP with SLA enforcement

### Visual Dashboard
- Dark-themed Command Center with 5 panels
- KPI cards (health score, auto-fix rate, cost saved)
- Severity distribution + remediation status charts
- Source health with SLA compliance visualization
- Pattern trends with directional indicators

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Platform | Snowflake |
| Orchestration | Snowflake Tasks (DAG) |
| Event Detection | Snowflake Streams |
| Classification | Rule-based (upgradeable to Cortex AI) |
| Visualization | Matplotlib |
| Language | SQL + Python |
| Notebook | Snowflake Notebooks |

---

## Database Schema

```
SELF_HEALING_PIPELINE/
├── RAW/
│   ├── ORDERS_LANDING          (main data table)
│   ├── INVENTORY_LANDING       (secondary table)
│   └── ORDERS_QUARANTINE       (quarantined bad rows)
├── MONITORING/
│   ├── QUALITY_CHECK_RESULTS   (all check outcomes)
│   ├── QUALITY_FAILURES_STREAM (real-time failure detection)
│   ├── ANOMALY_PATTERNS        (recurring issue tracking)
│   ├── DATA_SOURCE_REGISTRY    (multi-source health)
│   └── SOURCE_HEALTH_LOG       (source health history)
└── OPERATIONS/
    ├── ANOMALY_CLASSIFICATIONS (AI/rule classifications)
    ├── REMEDIATION_AUDIT_LOG   (full action history)
    ├── TABLE_DEPENDENCIES      (dependency graph)
    ├── DOWNSTREAM_PAUSE_LOG    (pause/resume log)
    ├── APPROVAL_QUEUE          (human review queue)
    ├── COST_PARAMETERS         (ROI configuration)
    ├── PIPELINE_ROI            (cost savings view)
    └── ESCALATED_ISSUES        (unresolved issues view)
```

---

## Quick Start

### Prerequisites
- Snowflake account (Trial or Enterprise)
- `ACCOUNTADMIN` role (or equivalent privileges)
- A warehouse (e.g., `COMPUTE_WH`)

### Setup
1. Open `self_healing_pipeline.ipynb` in Snowflake Notebooks
2. Run all cells from top to bottom
3. The pipeline will:
   - Create the database and schemas
   - Build all tables, procedures, and tasks
   - Seed sample data
   - Run quality checks
   - Classify and remediate issues
   - Display the dashboard

### Activate Autonomous Mode
```sql
ALTER TASK SELF_HEALING_PIPELINE.OPERATIONS.TASK_RUN_QUALITY_CHECKS RESUME;
```

### Test the Self-Healing
Run the "Inject Bad Data" cell to simulate an incident, then run "Run Full Self-Healing Cycle" to watch the pipeline detect, classify, and fix the issues automatically.

---

## Key Metrics (Demo Run)

| Metric | Value |
|--------|-------|
| Quality Checks Run | 900+ |
| Auto-Fix Success Rate | 100% |
| MTTR (Automated) | ~1.5 minutes |
| MTTR (Manual equivalent) | ~45 minutes |
| MTTR Improvement | 96.7% |
| Data Sources Monitored | 6 (API, S3, Kafka, SFTP) |

---

## How It Works

### Quality Check → Classification → Remediation

```sql
-- 1. Quality check detects 100% NULL rate on TOTAL_AMOUNT
-- Result: CRITICAL severity, FAILED

-- 2. Classifier determines:
--    Root Cause: "Upstream ETL failed to populate computed columns"
--    Action: "Recompute using QUANTITY * UNIT_PRICE"
--    Auto-remediable: TRUE

-- 3. Remediation engine executes:
UPDATE ORDERS_LANDING
SET TOTAL_AMOUNT = QUANTITY * UNIT_PRICE
WHERE TOTAL_AMOUNT IS NULL;
-- Result: RESOLVED ✓
```

### Pattern Prediction

```sql
-- After 180+ occurrences of NULL_CHECK failures:
-- Average interval: 0.17 hours (10 minutes)
-- Trend: WORSENING
-- Predicted next: 2024-07-19 08:30:00
-- Alert: "OVERDUE - likely failing now"
```

---

## Configuration

### Adjust Quality Thresholds
Edit the `RUN_QUALITY_CHECKS` procedure to change:
- NULL rate threshold (default: 5%)
- Minimum volume (default: 100 rows/24h)
- Expected column count (default: 10)
- Quantity range (default: 1-1000)

### Adjust Cost Parameters
```sql
UPDATE SELF_HEALING_PIPELINE.OPERATIONS.COST_PARAMETERS
SET PARAM_VALUE = 95.00
WHERE PARAM_NAME = 'AVG_ENGINEER_HOURLY_RATE';
```

### Add New Data Sources
```sql
INSERT INTO SELF_HEALING_PIPELINE.MONITORING.DATA_SOURCE_REGISTRY
    (SOURCE_NAME, SOURCE_TYPE, CONNECTION_STRING, TARGET_TABLE, 
     EXPECTED_FREQUENCY_MINUTES, SLA_FRESHNESS_MINUTES)
VALUES ('My New API', 'API', 'https://api.example.com/data', 
        'RAW.MY_TABLE', 30, 60);
```

---

## Upgrade to AI Classification

On paid Snowflake accounts, swap the rule-based classifier for Cortex AI:

```sql
-- Replace the CLASSIFY_ANOMALIES procedure body with:
SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', :prompt) INTO :ai_response;
```

This enables dynamic root cause analysis that adapts to new, unseen failure patterns.

---

## Project Structure

```
self-healing-data-pipeline/
├── self_healing_pipeline.ipynb   # Main notebook (all code + dashboard)
└── README.md                     # This file
```

---


