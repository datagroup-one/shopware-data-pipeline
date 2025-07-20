
##  Parquet Output to S3 for Redshift Spectrum & QuickSight

This setup enables clean, partitioned, analytics-ready data delivery to S3 using Kinesis Firehose and Lambda.

---

###  Step 1: Configure Firehose to Convert JSON to Parquet

**Firehose Setup (via Console or CLI):**

1. **Source**: Direct PUT or Kinesis Data Stream

2. **Destination**: Amazon S3

3. **Enable Data Transformation**:  Yes — point to your Lambda function

4. **Enable Format Conversion**:  Yes

5. **Input Format**: `JSON`

6. **Output Format**: `Parquet`

7. **Schema Configuration**:

   * Enable AWS Glue Data Catalog integration
   * Choose your predefined **Glue table schema** (see Step 2 below)

8. **Buffering Hints** (controls delivery frequency & file size):

   * Interval: 60–300 seconds
   * Size: 128 MB (optimal for Parquet files)

---

###  Step 2: Create a Glue Table for Schema Enforcement

Your Glue table schema must match the output format of your Lambda transformation.

####  Input Schema (from producer → Firehose):

```json
{
  "customer_id": "123",
  "interaction_type": "feedback",
  "channel": "email",
  "rating": "5",
  "message_excerpt": "Thanks for the quick support!",
  "timestamp": "1721462400"  // UNIX epoch format
}
```
####  Output Schema (from Lambda → Firehose → S3):

```json
{
  "customer_id": 123,
  "interaction_type": "feedback",
  "channel": "email",
  "rating": 5,
  "message_excerpt": "Thanks for the quick support!",
  "timestamp": "2024-07-20T00:00:00.000Z",
  "interaction_date": "2024-07-20",
  "interaction_hour": 0,
  "interaction_day_of_week": 5,
  "processed_at": "2024-07-20T00:00:03.456Z",
  "has_rating": true,
  "has_message": true,
  "has_channel": true,
  "year": 2024,
  "month": 7,
  "day": 20,
  "hour": 0
}
```

> Ensure all keys are top-level JSON fields before they reach Firehose.

---

### Glue Table DDL for Partitioned Parquet Storage

```sql
CREATE EXTERNAL TABLE crm_interactions (
  customer_id INT,
  interaction_type STRING,
  channel STRING,
  rating INT,
  message_excerpt STRING,
  timestamp TIMESTAMP,
  interaction_date DATE,
  interaction_hour INT,
  interaction_day_of_week INT,
  processed_at TIMESTAMP,
  has_rating BOOLEAN,
  has_message BOOLEAN,
  has_channel BOOLEAN
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION 's3://your-bucket/path/'
```

> Manually defining this schema in Glue (rather than relying on Crawlers) ensures schema consistency over time.

---

###  Step 3: Updated Lambda for Partitioning Fields


```python
dt = datetime.fromtimestamp(float(data['timestamp']))
return {
    ...
    'year': dt.year,
    'month': dt.month,
    'day': dt.day,
    'hour': dt.hour
}
```

These fields enable **automatic partitioning** in S3 and Redshift Spectrum.

---

###  Step 4: Create External Table in Redshift Spectrum

```sql
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'your_glue_db'
IAM_ROLE 'arn:aws:iam::<account>:role/your-redshift-spectrum-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE EXTERNAL TABLE spectrum_schema.crm_interactions
LIKE glue_db.crm_interactions;
```

> Make sure your IAM role has read access to the S3 path and Glue Data Catalog.

---

###  Step 5: Connect via QuickSight

1. In QuickSight, add Redshift Spectrum as a data source.
2. Choose the `crm_interactions` table.
3. Build dashboards using KPIs like:

   * Average rating
   * Interaction volume by type or channel
   * Peak hours of engagement
   * Day-of-week insights

---

###  Summary

| Component         | Responsibility                                         |
| ----------------- | ------------------------------------------------------ |
| Lambda            | Clean & enrich CRM data (adds timestamps, flags, etc.) |
| Firehose          | Converts to Parquet, partitions, delivers to S3        |
| Glue Data Catalog | Provides schema for validation & conversion            |
| Redshift Spectrum | Reads partitioned Parquet files from S3                |
| QuickSight        | Visualizes the insights with minimal latency           |

