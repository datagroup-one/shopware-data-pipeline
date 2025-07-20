
# Event Processor Lambda

This AWS Lambda function processes event records (e.g., user activity logs) from a stream such as Kinesis Firehose. It decodes, transforms, and enriches each record before returning the processed output in base64 format.


## Input Schema

Each record must be base64-encoded JSON with the following **required fields**:

```json
{
  "session_id": "string",
  "page": "string (URL or path)",
  "timestamp": "float or int (epoch seconds)",
  "user_id": "int (optional)",
  "device_type": "string (optional)",
  "browser": "string (optional)",
  "event_type": "string (optional)"
}
```

---

## Output Schema

Each successful record returns:

```json
{
  "recordId": "string",
  "result": "Ok",
  "data": "base64-encoded JSON string",
  "recordMetadata": {
    "partitionKeys": {
      "event_date": "YYYY-MM-DD"
    }
  }
}
```

Transformed record fields include:

* Parsed timestamp (`timestamp`, `timestamp_iso`, `event_date`, `event_hour`, etc.)
* Page metadata (`page_path`, `page_category`, `path_depth`, `has_query_params`)
* Booleans: `is_authenticated`, `is_mobile`, `is_engagement_event`
* Enriched `processed_at` timestamp

On failure, a record returns:

```json
{
  "recordId": "string",
  "result": "ProcessingFailed"
}
```

---

## Key Features

* Field validation with graceful error handling
* Smart categorization of pages (e.g., `product`, `auth`, `support`)
* Timestamp enrichment (hour, day, weekday, etc.)
* Device and engagement-type inference
* Partitioning by `event_date`

---
# Web Traffic KPI Pipeline

This AWS Lambda pipeline transforms raw web traffic logs into a minimal schema tailored for **KPI computation**. It is invoked via Kinesis Firehose and emits records in Parquet format partitioned by time for efficient querying via Athena or Redshift Spectrum.



##  Supported KPIs

The transformation supports **three key metric families**:

### 1. **Engagement Score**
- Tracks how many events represent meaningful interactions (clicks, scrolls, etc.)

### 2. **Session Metrics**

##  Output Fields

Only the **minimum necessary fields** are retained to power downstream analytics:

| Field               | Type      | Purpose                                |
|---------------------|-----------|----------------------------------------|
| `session_id`         | STRING    | Unique session identifier              |
| `user_id`            | INT       | User ID if logged in, else null        |
| `event_type`         | STRING    | Action type (click, scroll, etc.)      |
| `timestamp`          | TIMESTAMP | ISO8601 event timestamp                |
| `event_date`         | DATE      | Date portion of the timestamp          |
| `event_hour`         | INT       | Hour of day (0–23)                     |
| `event_day_of_week`  | INT       | Day of week (0=Monday, 6=Sunday)       |
| `is_authenticated`   | BOOLEAN   | True if user_id is present             |
| `is_engagement_event`| BOOLEAN   | True if event_type is an interaction   |

Partition Keys (used for S3/Athena partitioning):
- `year`, `month`, `day`, `hour` (all INT)

---

##  AWS Glue Table

```sql
CREATE EXTERNAL TABLE web_traffic_kpis (
  session_id STRING,
  user_id INT,
  event_type STRING,
  timestamp TIMESTAMP,
  event_date DATE,
  event_hour INT,
  event_day_of_week INT,
  is_authenticated BOOLEAN,
  is_engagement_event BOOLEAN
)
PARTITIONED BY (
  year INT,
  month INT,
  day INT,
  hour INT
)
STORED AS PARQUET
LOCATION 's3://your-kpi-bucket/web_traffic_kpis/';
````

Use this schema  Redshift Spectrum for performant queries.

---

##  Sample Queries

###  Engagement Score (Daily)

```sql
SELECT
  event_date,
  COUNT(*) AS total_events,
  SUM(CASE WHEN is_engagement_event THEN 1 ELSE 0 END) AS engagement_events,
  ROUND(100.0 * SUM(CASE WHEN is_engagement_event THEN 1 ELSE 0 END) / COUNT(*), 2) AS engagement_score
FROM web_traffic_kpis
GROUP BY event_date
ORDER BY event_date;
```

---

###  Session Metrics

```sql
SELECT
  event_date,
  COUNT(DISTINCT session_id) AS active_sessions,
  COUNT(*) AS total_events
FROM web_traffic_kpis
GROUP BY event_date;
```

---


## Deployment Notes

* Lambda is invoked via **Kinesis Firehose** (buffered ingestion).
* Firehose transforms records and writes to **S3** as **Parquet**, partitioned by time.
* Use  **Redshift Spectrum**, or **QuickSight** to analyze data.

---

##  Logging and Error Handling

* Records are validated and transformed individually.
* Any malformed or incomplete record is marked as `ProcessingFailed` (Firehose will back it up if configured).
* Logging level is dynamic via `LOG_LEVEL` env var (default: INFO).

---

##  Future Extensibility

Although this pipeline is KPI-minimal today, the Lambda is designed for easy extension:

* Add `page`, `device_type`, `browser`, or `path_depth` if needed for richer web analytics.
* Extend `is_engagement_event()` logic to adapt to business-specific behaviors.

```

