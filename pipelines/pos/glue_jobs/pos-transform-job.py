import sys
import boto3
from datetime import datetime, timezone
from decimal import Decimal
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import TimestampType


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TransactionETL:
    def __init__(self, glue_context, job_name):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.job_name = job_name
        self.s3_client = boto3.client('s3')
        
        # Configuration
        self.source_bucket = "shopware.bucket"
        self.source_prefix = f"raw-data/pos/{datetime.now().strftime('%Y/%m/%d')}/"
        self.archive_bucket = "shopware.bucket"
        self.archive_prefix = "archive/pos/"
        
        self.redshift_connection = "redshift-connection"
        self.redshift_schema = "public"
        self.redshift_table = "processed_pos"
        
        # Data quality thresholds
        self.min_revenue_threshold = 0.01
        self.max_revenue_threshold = 10000.0
        
    def get_source_files(self):
        """Get list of CSV files to process from S3"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.source_bucket,
                Prefix=self.source_prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.csv'):
                        files.append(f"s3://{self.source_bucket}/{obj['Key']}")
            
            logger.info(f"Found {len(files)} files to process")
            return files
            
        except Exception as e:
            logger.error(f"Error listing source files: {str(e)}")
            raise
    
    def read_source_data(self, file_paths):
        """Read and combine all source CSV files"""
        try:
            
            # Read all files
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(file_paths)
            
            # Add metadata columns
            df = df.withColumn("file_name", input_file_name()) \
                   .withColumn("processing_timestamp", current_timestamp()) \
                   .withColumn("job_run_id", lit(self.job_name))
            
            logger.info(f"Read {df.count()} records from source files")
            return df
            
        except Exception as e:
            logger.error(f"Error reading source data: {str(e)}")
            raise
    
    def validate_data_quality(self, df):
        """Perform comprehensive data quality checks"""
        total_records = df.count()
        logger.info(f"Starting data quality validation for {total_records} records")
        
        # Check for duplicates
        duplicate_count = df.groupBy("transaction_id").count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate transaction IDs")
        
        # Business rule validations
        invalid_revenue = df.filter(
            (col("revenue") < self.min_revenue_threshold) | 
            (col("revenue") > self.max_revenue_threshold)
        ).count()
        
        if invalid_revenue > 0:
            logger.warning(f"Found {invalid_revenue} records with invalid revenue values")
        
        # Check for invalid quantities
        invalid_quantity = df.filter((col("quantity") <= 0) | (col("quantity") > 1000)).count()
        if invalid_quantity > 0:
            logger.warning(f"Found {invalid_quantity} records with invalid quantities")
        
        logger.info("Data quality validation completed")
        return df
    
    def transform_data(self, df):
        """Apply comprehensive transformations for analytics readiness"""
        logger.info("Starting data transformations")
        logger.info("Sample timestamp values:")
        
        df.printSchema()
        df.select("timestamp").show(5, truncate=False)
        
     
        # 1. Convert timestamp to proper datetime and extract date components
        df = df.withColumn("transaction_datetime", 
                          from_unixtime(col("timestamp")).cast(TimestampType()))
        logger.info("Sample timestamp conversions:")
        df.select("timestamp", "transaction_datetime").show(5, truncate=False)
        
        df = df.withColumn("transaction_date", to_date(col("transaction_datetime"))) \
               .withColumn("transaction_year", year(col("transaction_datetime"))) \
               .withColumn("transaction_month", month(col("transaction_datetime"))) \
               .withColumn("transaction_day", dayofmonth(col("transaction_datetime"))) \
               .withColumn("transaction_hour", hour(col("transaction_datetime"))) \
               .withColumn("day_of_week", dayofweek(col("transaction_datetime"))) \
               .withColumn("week_of_year", weekofyear(col("transaction_datetime")))
        
        # 2. Handle null discount values and create discount indicators
        df = df.withColumn("discount_applied", 
                          when(col("discount_applied").isNull(), 0.0)
                          .otherwise(col("discount_applied")))
        
        df = df.withColumn("has_discount", 
                          when(col("discount_applied") > 0, True).otherwise(False))
        
        # 3. Calculate derived financial metrics
        df = df.withColumn("gross_revenue", col("revenue") + col("discount_applied")) \
               .withColumn("net_revenue", col("revenue")) \
               .withColumn("discount_percentage", 
                          when(col("gross_revenue") > 0, 
                               col("discount_applied") / col("gross_revenue") * 100)
                          .otherwise(0.0)) \
               .withColumn("revenue_per_item", col("revenue") / col("quantity"))
        
        # 4. Add transaction categorization
        df = df.withColumn("transaction_size_category",
                          when(col("revenue") < 50, "Small")
                          .when(col("revenue") < 200, "Medium")
                          .when(col("revenue") < 500, "Large")
                          .otherwise("Extra Large"))
        
        df = df.withColumn("quantity_category",
                          when(col("quantity") == 1, "Single")
                          .when(col("quantity") <= 5, "Small Batch")
                          .when(col("quantity") <= 10, "Medium Batch")
                          .otherwise("Large Batch"))
        
        # 5. Add rolling window analytics (useful for time series analysis)
        window_spec = Window.partitionBy("store_id").orderBy("transaction_datetime")
        
        df = df.withColumn("running_total_revenue", 
                          sum("revenue").over(window_spec)) \
               .withColumn("transaction_rank_in_store", 
                          row_number().over(window_spec))
        
        # 6. Store and product performance indicators
        store_window = Window.partitionBy("store_id", "transaction_date")
        product_window = Window.partitionBy("product_id", "transaction_date")
        
        df = df.withColumn("daily_store_transaction_count", 
                          count("*").over(store_window)) \
               .withColumn("daily_store_revenue", 
                          sum("revenue").over(store_window)) \
               .withColumn("daily_product_sales", 
                          sum("quantity").over(product_window)) \
               .withColumn("daily_product_revenue", 
                          sum("revenue").over(product_window))
        
        # 7. Add data quality flags
        df = df.withColumn("is_valid_transaction",
                          when((col("revenue") >= self.min_revenue_threshold) &
                               (col("revenue") <= self.max_revenue_threshold) &
                               (col("quantity") > 0) &
                               (col("quantity") <= 1000), True)
                          .otherwise(False))
        
        # 8. Create partition columns for efficient querying
        df = df.withColumn("year_month", 
                          concat(col("transaction_year"), 
                                lpad(col("transaction_month"), 2, "0")))
        
        # 9. Final column selection and ordering
        final_columns = [
            "transaction_id", "store_id", "product_id", "quantity",
            "revenue", "discount_applied", "gross_revenue", "net_revenue",
            "discount_percentage", "revenue_per_item", "has_discount",
            "transaction_datetime", "transaction_date", "transaction_year",
            "transaction_month", "transaction_day", "transaction_hour",
            "day_of_week", "week_of_year", "year_month",
            "transaction_size_category", "quantity_category",
            "running_total_revenue", "transaction_rank_in_store",
            "daily_store_transaction_count", "daily_store_revenue",
            "daily_product_sales", "daily_product_revenue",
            "is_valid_transaction", "processing_timestamp", "job_run_id"
        ]
        
        df = df.select(*final_columns)
        
        # 10. Cache for performance if doing multiple operations
        df.cache()
        
        logger.info(f"Transformations completed. Final dataset has {df.count()} records")
        return df
    
    def write_to_redshift(self, df):
        """Write transformed data to Redshift with optimizations"""
        logger.info("Writing data to Redshift")
        
        try:
            # Write to Redshift using Glue connection
            self.glue_context.write_dynamic_frame.from_options(
                frame=DynamicFrame.fromDF(df, self.glue_context, "redshift_frame"),
                connection_type="redshift",
                connection_options={
                    "useConnectionProperties": "true",
                    "connectionName": self.redshift_connection,
                    "dbtable": f"{self.redshift_schema}.{self.redshift_table}",
                    "redshiftTmpDir": "s3://shopware.bucket/redshift-temp/"
                },
                transformation_ctx="redshift_write"
            )
                    
            logger.info("Successfully wrote data to Redshift")
            
        except Exception as e:
            logger.error(f"Error writing to Redshift: {str(e)}")
            raise
        
    def archive_processed_files(self, processed_files):
        """Move processed files to archive bucket"""
        logger.info(f"Archiving {len(processed_files)} processed files")
        
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            
            for file_path in processed_files:
                # Extract bucket and key from s3 path
                parts = file_path.replace("s3://", "").split("/", 1)
                source_bucket = parts[0]
                source_key = parts[1]
                
                # Create archive key with timestamp
                filename = source_key.split("/")[-1]
                archive_key = f"{self.archive_prefix}{timestamp}/{filename}"
                
                # Copy to archive bucket
                self.s3_client.copy_object(
                    CopySource={'Bucket': source_bucket, 'Key': source_key},
                    Bucket=self.archive_bucket,
                    Key=archive_key
                )
                
                # Delete from source
                self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
                
                logger.info(f"Archived {source_key} to {archive_key}")
                
        except Exception as e:
            logger.error(f"Error archiving files: {str(e)}")
            raise
    
    def generate_processing_summary(self, df):
        """Generate processing summary for monitoring"""
        summary = {
            "total_records": df.count(),
            "valid_transactions": df.filter(col("is_valid_transaction")).count(),
            "total_revenue": df.agg(sum("revenue")).collect()[0][0],
            "unique_stores": df.select("store_id").distinct().count(),
            "unique_products": df.select("product_id").distinct().count(),
            "date_range": {
                "min_date": df.agg(min("transaction_date")).collect()[0][0],
                "max_date": df.agg(max("transaction_date")).collect()[0][0]
            },
            "processing_timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Processing Summary: {summary}")
        return summary
    
    def run(self):
        """Main execution method"""
        try:
            logger.info(f"Starting ETL job: {self.job_name}")
            
            # Get source files
            source_files = self.get_source_files()
            if not source_files:
                logger.info("No files to process")
                return
            
            # Read source data
            raw_df = self.read_source_data(source_files)
            
            # Validate data quality
            validated_df = self.validate_data_quality(raw_df)
            
            # Transform data
            transformed_df = self.transform_data(validated_df)
            
            # Generate processing summary
            self.generate_processing_summary(transformed_df)
            
            # Write to Redshift
            self.write_to_redshift(transformed_df)
            
            # Archive processed files
            self.archive_processed_files(source_files)
            
            logger.info("ETL job completed successfully")
            
        except Exception as e:
            logger.error(f"ETL job failed: {str(e)}")
            raise


def main():
    """Main function to run the Glue job"""
    # Get job parameters
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    # Initialize Spark and Glue contexts
    sc = SparkContext()
    glue_context = GlueContext(sc)
    
    # Configure Spark for optimal performance
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # Initialize and run job
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    try:
        etl = TransactionETL(glue_context, args['JOB_NAME'])
        etl.run()
        job.commit()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        sc.stop()


if __name__ == "__main__":
    main()