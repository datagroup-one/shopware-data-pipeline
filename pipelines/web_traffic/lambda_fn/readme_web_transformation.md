# Web Traffic KPI Pipeline

AWS serverless pipeline that transforms raw web traffic logs into a **minimal KPI-focused schema**. Invoked via Kinesis Firehose, outputs Parquet files partitioned for efficient analytics with Redshift Spectrum.

## Architecture Flow
**Raw Events** → **ECS(FARGATE)** → **Kinesis Firehose** → **Lambda Transform** → **S3 Parquet** → **Glue Catalog + Redshift** → **Analytics**

## Supported KPIs

### 1. Engagement Score
Tracks meaningful user interactions (clicks, scrolls, form submissions) as percentage of total events.

### 2. Session Metrics  
Measures active sessions, session duration, and event frequency per session.

### 3. Temporal Patterns
Analyzes traffic by hour, day of week, and date for trend identification.

## Minimal Output Schema

Only essential fields retained for KPI computation:

| Field | Type | Purpose |
|-------|------|---------|
| `session_id` | STRING | Unique session identifier |
| `user_id` | INT | User ID (null if anonymous) |
| `event_type` | STRING | Action type (click, scroll, etc.) |
| `timestamp` | STRING | ISO8601 event timestamp |
| `event_date` | STRING | Date portion (YYYY-MM-DD) |
| `event_hour` | INT | Hour of day (0-23) |
| `event_day_of_week` | INT | Day of week (0=Monday, 6=Sunday) |
| `is_authenticated` | BOOLEAN | True if user_id present |
| `is_engagement_event` | BOOLEAN | True if meaningful interaction |

**Partition Keys**: `year`, `month`, `day`, `hour` (INT) - for S3/Athena optimization

## Glue Table Schema

```sql
CREATE EXTERNAL TABLE web_traffic_logs (
  session_id STRING,
  user_id INT,
  event_type STRING,
  timestamp STRING,
  event_date STRING,
  event_hour INT,
  event_day_of_week INT,
  is_authenticated BOOLEAN,
  is_engagement_event BOOLEAN
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION 's3://shopware.bucket/curated-data/web-logs/valid/';
```

## Sample KPI Queries

### Daily Engagement Score
```sql
SELECT 
  event_date,
  COUNT(*) as total_events,
  SUM(CASE WHEN is_engagement_event THEN 1 ELSE 0 END) as engagement_events,
  ROUND(100.0 * SUM(CASE WHEN is_engagement_event THEN 1 ELSE 0 END) / COUNT(*), 2) as engagement_score
FROM web_traffic_logs
GROUP BY event_date
ORDER BY event_date;
```

### Hourly Session Activity
```sql
SELECT 
  event_hour,
  COUNT(DISTINCT session_id) as active_sessions,
  COUNT(*) as total_events,
  ROUND(COUNT(*) / COUNT(DISTINCT session_id), 2) as events_per_session
FROM web_traffic_logs
WHERE event_date = CURRENT_DATE
GROUP BY event_hour
ORDER BY event_hour;
```

## Engineering Practices

### **Reliability**
- Individual record validation with graceful failure handling
- Failed records marked as `ProcessingFailed` for Firehose backup
- CloudWatch + SNS monitoring with automated alerts
- Configurable logging via `LOG_LEVEL` environment variable

### **Performance**  
- Batch record processing in single Lambda invocation
- Lightweight transformations with minimal dependencies
- Efficient Base64 encoding/decoding for data transport
- Time-based partitioning for optimal query performance

### **Code Quality**
- Modular functions with single responsibilities
- Comprehensive error boundaries and type validation  
- Structured logging with correlation IDs
- Environment-based configuration

## Deployment

1. **Lambda**: Deploy with Firehose invoke permissions
2. **Firehose**: Configure Lambda as data transformation processor
3. **S3**: Set up bucket with time-based partitioning
4. **Glue**: Create table schema for data catalog
5. **Monitoring**: Configure CloudWatch alarms + SNS notifications

## Extensibility

Pipeline designed for easy extension:
- Add `page_category`, `device_type`, `browser` fields for richer analytics
- Extend `is_engagement_event()` logic for business-specific behaviors
- Modify partitioning strategy for different query patterns
- Integrate additional downstream analytics tools (QuickSight, Redshift)


## Architecture Strengths


###  **Serverless & Auto-Scaling**
- **Zero infrastructure management** - No clusters to provision or maintain
- **Elastic scaling** - Handles traffic spikes from 10 to 10M events seamlessly
- **Pay-per-use model** - Only pay for actual data processing and storage

###  **Cost-Optimized Storage**
- **Columnar Parquet format** - 90% smaller files vs JSON
- **Intelligent partitioning** - Query only relevant time slices
- **S3 lifecycle policies** - Automatic archival to cheaper storage tiers
- **No idle cluster costs** - Eliminates traditional data warehouse overhead

### **Query Performance**
- **Predicate pushdown** - Scan only required columns and partitions
- **Multi-engine support** - Query with Athena, Redshift Spectrum, or QuickSight
- **Sub-second analytics** - Optimized for KPI computation workloads

