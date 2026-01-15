# Reddit Data Pipeline - ETL Lakehouse Project

A data engineering project implementing a modern data lakehouse architecture for Reddit posts using Apache Airflow, PySpark, Delta Lake, and Great Expectations.

## Table of Contents

1. [Architecture](#architecture)
2. [Technology Stack](#technology-stack)
3. [Data Flow](#data-flow)
4. [Table Structure](#table-structure)
5. [CI/CD Pipeline](#cicd-pipeline)
6. [Getting Started](#getting-started)

---

## Architecture

![Project Architecture](Project%20Architechture.png)

### Dashboard Sample

![Dashboard Sample](report/sample/dashboard.png)

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestration | Apache Airflow (Astronomer Runtime 12.6.0) | Workflow scheduling & monitoring |
| Processing | Apache Spark 3.5.3 | Distributed data processing with Structured Streaming |
| Storage | Delta Lake 3.2.1 | ACID transactions, CDC, time travel |
| Data Quality | Great Expectations 1.0.0 | Validation with quarantine system |
| Object Storage | MinIO | S3-compatible lakehouse storage |
| Query Engine | Trino | Distributed SQL on Delta Lake |
| CI/CD | GitHub Actions | Automated build & deployment |

---

## Data Flow

The pipeline follows a **medallion architecture** pattern:

```
Raw CSV Files → Bronze Layer → Silver Layer → Gold Layer
    (Landing)   (Validated)    (Cleaned)      (Star Schema)
```

### Key Features

- **Bronze**: CSV ingestion with Great Expectations validation, failing rows quarantined
- **Silver**: Type casting, CDC enabled, MERGE upserts with conditional updates
- **Gold**: Star schema with fact table (`fact_posts_gl`) and dimensions (`dim_authors_gl`, `dim_flairs_gl`, `dim_domains_gl`)

---

## Table Structure

### Database: `reddit_db`

#### Bronze Layer: `reddit_posts_bz`

| Column | Type | Description |
|--------|------|-------------|
| `post_id` | STRING | Unique Reddit post identifier |
| `title`, `author`, `selftext` | STRING | Post content fields |
| `score`, `comments` | INT | Engagement metrics |
| `upvote_ratio` | DOUBLE | Upvote ratio (0.0-1.0) |
| `flair`, `domain`, `url` | STRING | Metadata fields |
| `is_video`, `is_self` | STRING | Raw boolean strings |
| `created_utc` | STRING | Post creation timestamp |
| `extracted_time`, `load_time` | TIMESTAMP | Pipeline timestamps |

#### Silver Layer: `reddit_posts_sl`

Same as Bronze with transformations:
- `is_video`, `is_self` → BOOLEAN
- `created_utc` → TIMESTAMP
- CDC enabled (`delta.enableChangeDataFeed = true`)
- Added `update_time` column

#### Gold Layer: Star Schema

**`fact_posts_gl`**: Core metrics with derived `format` column (text/video/Others)

**Dimensions**: `dim_authors_gl`, `dim_flairs_gl`, `dim_domains_gl`

#### `data_quality_quarantine`

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | STRING | Source table |
| `gx_batch_id` | STRING | Batch identifier |
| `violated_rules` | STRING | Failed expectations |
| `raw_data` | STRING | JSON of failed row |
| `ingestion_time` | TIMESTAMP | Quarantine time |

---

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/deploy.yml`) automates deployment:

### Workflow Triggers
- Push to `main` branch

### Pipeline Steps

1. **Build & Push**
   - Multi-architecture Docker build (amd64/arm64)
   - Push to Docker Hub with GitHub Actions cache

2. **Deploy**
   - Connect via Tailscale VPN
   - SSH to server, pull latest code and image
   - Restart services with `docker compose up -d`
   - Auto-sync Airflow permissions

### Required Secrets
- `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`
- `TAILSCALE_AUTHKEY`
- `SERVER_HOST`, `SERVER_USER`, `SSH_PRIVATE_KEY`

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Astronomer CLI (optional)

### Quick Start

```bash
# Clone and start
git clone <repository-url>
cd airflow-02-reddit-project
astro dev start  # or: docker-compose up -d

# Access Airflow UI
# URL: http://localhost:8080 (admin/admin)
```

### Environment Variables

```bash
AWS_ENDPOINT_URL=http://minio:9000
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=admin_password
MINIO_BUCKET=reddit-warehouse
```

---

## Best Practices

- **Idempotency**: All operations support re-runs without duplication
- **Exactly-Once Processing**: Checkpoint-based streaming
- **Data Quality Gates**: Validation before ingestion with quarantine
- **Incremental Processing**: CDC-based updates
- **Schema Evolution**: Delta Lake handles changes gracefully
