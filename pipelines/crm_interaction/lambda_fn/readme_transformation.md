## For **Parquet output to S3 for Redshift Spectrum & QuickSight**, here’s what to do:


## Step 1: Configure Firehose to Convert JSON to Parquet

configure Firehose like this:

###  Firehose Setup (console):

1. **Source**: Direct Put / Kinesis Data Stream
2. **Destination**: S3
3. **Enable data transformation**: Yes (point to your Lambda)
4. **Enable format conversion**:  Yes
5. **Input format**: JSON
6. **Output format**: Parquet
7. **Schema configuration**:

   * Enable AWS Glue Data Catalog integration
   * Point to your predefined **Glue table schema** (see Step 2)
8. **Buffering hints**:

   * Interval: 60–300 seconds (adjust for latency vs. throughput)
   * Size: 128 MB (Parquet optimal size)

---

## Step 2: Create Glue Table for Schema Enforcement

You need a Glue table that matches your transformed records.

Here’s the Glue schema based on the record format from lambda:

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

>  You can also create this Glue table manually or via a Crawler initially, but **manually defining it ensures stability** for streaming data.

---

## Step 3: Update Your Lambda to Add Partition Keys (Optional)

If you want Firehose to **automatically partition files** (e.g. by `year`, `month`, `day`, `hour`), you'll need to **emit these as top-level fields** in your transformed JSON.

Add this to your `transform_crm_record` function:

```python
# Add year/month/day/hour fields for S3 partitioning
dt = datetime.fromtimestamp(float(data['timestamp']))
return {
    ...
    'year': dt.year,
    'month': dt.month,
    'day': dt.day,
    'hour': dt.hour
}
```

---

## Step 4: Redshift Spectrum External Table

Now create an external table in Redshift:

```sql
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'your_glue_db'
IAM_ROLE 'arn:aws:iam::<account>:role/your-redshift-spectrum-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE EXTERNAL TABLE spectrum_schema.crm_interactions
LIKE glue_db.crm_interactions;
```

---

##  Step 5: Use QuickSight

1. In QuickSight, connect to the **Redshift Spectrum** data source.
2. Use your `crm_interactions` table.
3. Build dashboards using KPIs like:

   * Average rating
   * Volume by interaction type or channel
   * Resolution time if you track that
   * Time-series trends by `interaction_date` or `hour`

---

##  Final Notes

*  Lambda  **outputs clean JSON** for Firehose.
*  Firehose does **Parquet conversion + partitioning + delivery to S3**.
*  Glue ensures schema consistency.
*  Spectrum/QuickSight reads from S3 without full loading into Redshift.

--