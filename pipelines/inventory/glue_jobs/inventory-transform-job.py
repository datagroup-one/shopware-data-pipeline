import sys
import boto3
from datetime import datetime, timezone
from decimal import Decimal
import logging
import json

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InventoryETL:
    def __init__(self, glue_context, job_name):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.job_name = job_name
        self.s3_client = boto3.client('s3')
        
        # Configuration
        self.source_bucket = "your-inventory-source-bucket"
        self.source_prefix = "raw-inventory/"
        self.archive_bucket = "your-inventory-archive-bucket"
        self.archive_prefix = "archived-inventory/"
        
        self.redshift_connection = "redshift-connection-name"
        self.redshift_schema = "public"
        self.redshift_table = "inventory_processed"
        
        # Data quality thresholds
        self.max_null_percentage = 0.05  # 5% max nulls allowed
        self.min_stock_level = 0
        self.max_stock_level = 100000
        self.min_restock_threshold = 1
        self.max_restock_threshold = 1000
        
    def get_source_files(self):
        """Get list of JSON files to process from S3"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.source_bucket,
                Prefix=self.source_prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.json') or obj['Key'].endswith('.jsonl'):
                        files.append(f"s3://{self.source_bucket}/{obj['Key']}")
            
            logger.info(f"Found {len(files)} files to process")
            return files
            
        except Exception as e:
            logger.error(f"Error listing source files: {str(e)}")
            raise
    
    def read_source_data(self, file_paths):
        """Read and combine all source JSON files"""
        try:
            # Define schema for better performance and data quality
            schema = StructType([
                StructField("inventory_id", IntegerType(), False),
                StructField("product_id", IntegerType(), False),
                StructField("warehouse_id", IntegerType(), False),
                StructField("stock_level", IntegerType(), False),
                StructField("restock_threshold", IntegerType(), True),
                StructField("last_updated", DoubleType(), False)
            ])
            
            # Read all JSON files
            df = self.spark.read \
                .option("multiLine", "true") \
                .option("mode", "PERMISSIVE") \
                .schema(schema) \
                .json(file_paths)
            
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
        duplicate_count = df.groupBy("inventory_id").count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate inventory IDs")
        
        # Check null percentages for critical columns
        critical_columns = ["inventory_id", "product_id", "warehouse_id", "stock_level", "last_updated"]
        
        for column in critical_columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = null_count / total_records
            
            if null_percentage > self.max_null_percentage:
                raise ValueError(f"Column {column} has {null_percentage:.2%} null values, exceeding threshold of {self.max_null_percentage:.2%}")
            
            logger.info(f"Column {column}: {null_percentage:.2%} null values")
        
        # Business rule validations
        invalid_stock = df.filter(
            (col("stock_level") < self.min_stock_level) | 
            (col("stock_level") > self.max_stock_level)
        ).count()
        
        if invalid_stock > 0:
            logger.warning(f"Found {invalid_stock} records with invalid stock levels")
        
        # Check for invalid restock thresholds (excluding nulls)
        invalid_restock = df.filter(
            col("restock_threshold").isNotNull() &
            ((col("restock_threshold") < self.min_restock_threshold) | 
             (col("restock_threshold") > self.max_restock_threshold))
        ).count()
        
        if invalid_restock > 0:
            logger.warning(f"Found {invalid_restock} records with invalid restock thresholds")
        
        # Check for future timestamps
        current_timestamp_unix = datetime.now(timezone.utc).timestamp()
        future_timestamps = df.filter(col("last_updated") > current_timestamp_unix).count()
        
        if future_timestamps > 0:
            logger.warning(f"Found {future_timestamps} records with future timestamps")
        
        logger.info("Data quality validation completed")
        return df
    
    def transform_data(self, df):
        """Apply comprehensive transformations for analytics readiness"""
        logger.info("Starting data transformations")
        
        # 1. Convert timestamp to proper datetime and extract date components
        df = df.withColumn("last_updated_datetime", 
                          from_unixtime(col("last_updated")).cast(TimestampType()))
        
        df = df.withColumn("last_updated_date", to_date(col("last_updated_datetime"))) \
               .withColumn("update_year", year(col("last_updated_datetime"))) \
               .withColumn("update_month", month(col("last_updated_datetime"))) \
               .withColumn("update_day", dayofmonth(col("last_updated_datetime"))) \
               .withColumn("update_hour", hour(col("last_updated_datetime"))) \
               .withColumn("update_day_of_week", dayofweek(col("last_updated_datetime"))) \
               .withColumn("update_week_of_year", weekofyear(col("last_updated_datetime")))
        
        # 2. Handle null restock thresholds with intelligent defaults
        # Calculate median restock threshold per warehouse for null imputation
        warehouse_median_restock = df.filter(col("restock_threshold").isNotNull()) \
            .groupBy("warehouse_id") \
            .agg(expr("percentile_approx(restock_threshold, 0.5)").alias("median_restock_threshold"))
        
        # Join back and fill nulls
        df = df.join(warehouse_median_restock, "warehouse_id", "left")
        df = df.withColumn("restock_threshold_filled", 
                          when(col("restock_threshold").isNull(), 
                               coalesce(col("median_restock_threshold"), lit(50)))
                          .otherwise(col("restock_threshold")))
        
        # 3. Create inventory status indicators
        df = df.withColumn("needs_restock", 
                          when(col("stock_level") <= col("restock_threshold_filled"), True)
                          .otherwise(False))
        
        df = df.withColumn("stock_status",
                          when(col("stock_level") == 0, "Out of Stock")
                          .when(col("stock_level") <= col("restock_threshold_filled"), "Low Stock")
                          .when(col("stock_level") <= col("restock_threshold_filled") * 2, "Medium Stock")
                          .otherwise("High Stock"))
        
        # 4. Calculate inventory metrics
        df = df.withColumn("stock_above_threshold", 
                          greatest(col("stock_level") - col("restock_threshold_filled"), lit(0)))
        
        df = df.withColumn("stock_coverage_ratio",
                          when(col("restock_threshold_filled") > 0, 
                               col("stock_level") / col("restock_threshold_filled"))
                          .otherwise(lit(0.0)))
        
        df = df.withColumn("days_since_update",
                          datediff(current_date(), col("last_updated_date")))
        
        # 5. Create inventory risk categories
        df = df.withColumn("inventory_risk_level",
                          when(col("stock_level") == 0, "Critical")
                          .when(col("stock_coverage_ratio") < 0.5, "High")
                          .when(col("stock_coverage_ratio") < 1.0, "Medium")
                          .when(col("stock_coverage_ratio") < 2.0, "Low")
                          .otherwise("Minimal"))
        
        # 6. Add warehouse-level analytics
        warehouse_window = Window.partitionBy("warehouse_id")
        
        df = df.withColumn("warehouse_total_products", 
                          count("product_id").over(warehouse_window)) \
               .withColumn("warehouse_total_stock", 
                          sum("stock_level").over(warehouse_window)) \
               .withColumn("warehouse_avg_stock", 
                          avg("stock_level").over(warehouse_window)) \
               .withColumn("warehouse_low_stock_count", 
                          sum(when(col("needs_restock"), 1).otherwise(0)).over(warehouse_window))
        
        # 7. Add product-level analytics across warehouses
        product_window = Window.partitionBy("product_id")
        
        df = df.withColumn("product_total_stock_all_warehouses", 
                          sum("stock_level").over(product_window)) \
               .withColumn("product_warehouse_count", 
                          count("warehouse_id").over(product_window)) \
               .withColumn("product_avg_stock_per_warehouse", 
                          avg("stock_level").over(product_window)) \
               .withColumn("product_min_stock_warehouse", 
                          min("stock_level").over(product_window)) \
               .withColumn("product_max_stock_warehouse", 
                          max("stock_level").over(product_window))
        
        # 8. Calculate inventory turnover indicators
        df = df.withColumn("stock_level_category",
                          when(col("stock_level") == 0, "Empty")
                          .when(col("stock_level") < 10, "Very Low")
                          .when(col("stock_level") < 50, "Low")
                          .when(col("stock_level") < 200, "Medium")
                          .when(col("stock_level") < 500, "High")
                          .otherwise("Very High"))
        
        # 9. Create time-based partitioning for efficient querying
        df = df.withColumn("year_month", 
                          concat(col("update_year"), 
                                lpad(col("update_month"), 2, "0")))
        
        # 10. Add freshness indicators
        df = df.withColumn("data_freshness",
                          when(col("days_since_update") == 0, "Current")
                          .when(col("days_since_update") <= 1, "Recent")
                          .when(col("days_since_update") <= 7, "Weekly")
                          .when(col("days_since_update") <= 30, "Monthly")
                          .otherwise("Stale"))
        
        # 11. Calculate warehouse efficiency metrics
        df = df.withColumn("warehouse_stock_efficiency",
                          col("warehouse_total_stock") / col("warehouse_total_products"))
        
        df = df.withColumn("warehouse_restock_rate",
                          col("warehouse_low_stock_count") / col("warehouse_total_products"))
        
        # 12. Add data quality flags
        df = df.withColumn("is_valid_inventory",
                          when((col("stock_level") >= self.min_stock_level) &
                               (col("stock_level") <= self.max_stock_level) &
                               (col("restock_threshold_filled") >= self.min_restock_threshold) &
                               (col("restock_threshold_filled") <= self.max_restock_threshold) &
                               (col("days_since_update") <= 30), True)
                          .otherwise(False))
        
        df = df.withColumn("has_null_restock_threshold",
                          when(col("restock_threshold").isNull(), True).otherwise(False))
        
        # 13. Final column selection and ordering
        final_columns = [
            "inventory_id", "product_id", "warehouse_id", "stock_level",
            "restock_threshold", "restock_threshold_filled", "needs_restock",
            "stock_status", "inventory_risk_level", "stock_level_category",
            "stock_above_threshold", "stock_coverage_ratio",
            "last_updated_datetime", "last_updated_date", "days_since_update",
            "data_freshness", "update_year", "update_month", "update_day",
            "update_hour", "update_day_of_week", "update_week_of_year", "year_month",
            "warehouse_total_products", "warehouse_total_stock", "warehouse_avg_stock",
            "warehouse_low_stock_count", "warehouse_stock_efficiency", "warehouse_restock_rate",
            "product_total_stock_all_warehouses", "product_warehouse_count",
            "product_avg_stock_per_warehouse", "product_min_stock_warehouse",
            "product_max_stock_warehouse", "is_valid_inventory", "has_null_restock_threshold",
            "processing_timestamp", "job_run_id"
        ]
        
        df = df.select(*final_columns)
        
        # 14. Cache for performance if doing multiple operations
        df.cache()
        
        logger.info(f"Transformations completed. Final dataset has {df.count()} records")
        return df
    
    def write_to_redshift(self, df):
        """Write transformed data to Redshift with optimizations"""
        logger.info("Writing data to Redshift")
        
        try:
            # Write to Redshift using Glue's built-in connector
            df.write \
                .format("com.databricks.spark.redshift") \
                .option("url", f"jdbc:redshift://{self.redshift_connection}") \
                .option("dbtable", f"{self.redshift_schema}.{self.redshift_table}") \
                .option("tempdir", "s3://your-temp-bucket/redshift-temp/") \
                .option("aws_iam_role", "arn:aws:iam::account:role/RedshiftRole") \
                .mode("overwrite") \
                .save()
            
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
        
        # Calculate summary statistics
        total_records = df.count()
        out_of_stock_count = df.filter(col("stock_level") == 0).count()
        low_stock_count = df.filter(col("needs_restock")).count()
        
        summary = {
            "total_inventory_records": total_records,
            "valid_records": df.filter(col("is_valid_inventory")).count(),
            "out_of_stock_items": out_of_stock_count,
            "low_stock_items": low_stock_count,
            "total_stock_units": df.agg(sum("stock_level")).collect()[0][0],
            "unique_products": df.select("product_id").distinct().count(),
            "unique_warehouses": df.select("warehouse_id").distinct().count(),
            "null_restock_thresholds": df.filter(col("has_null_restock_threshold")).count(),
            "inventory_risk_distribution": df.groupBy("inventory_risk_level").count().collect(),
            "warehouse_performance": df.groupBy("warehouse_id").agg(
                count("*").alias("product_count"),
                sum("stock_level").alias("total_stock"),
                sum(when(col("needs_restock"), 1).otherwise(0)).alias("items_needing_restock")
            ).collect(),
            "data_freshness_distribution": df.groupBy("data_freshness").count().collect(),
            "processing_timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Processing Summary: {summary}")
        return summary
    
    def run(self):
        """Main execution method"""
        try:
            logger.info(f"Starting Inventory ETL job: {self.job_name}")
            
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
            
            logger.info("Inventory ETL job completed successfully")
            
        except Exception as e:
            logger.error(f"Inventory ETL job failed: {str(e)}")
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
        etl = InventoryETL(glue_context, args['JOB_NAME'])
        etl.run()
        job.commit()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        sc.stop()


if __name__ == "__main__":
    main()