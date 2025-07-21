import os
import json
import time
import logging
import requests
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, List, Optional, Tuple
from io import BytesIO
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                'status': 'healthy',
                'service': 'crm-producer',
                'timestamp': datetime.utcnow().isoformat(),
                'version': '2.0'
            }
            self.wfile.write(json.dumps(response).encode())
            logger.info("Health check requested - returning healthy status")
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'Not found'}).encode())

    def log_message(self, format, *args):
        pass

class CRMProducer:
    # Expected fields for CRM data validation
    EXPECTED_FIELDS = {"customer_id", "interaction_type", "timestamp"}
    
    def __init__(self):
        self.api_url = os.getenv('API_URL')
        self.stream_name = os.getenv('FIREHOSE_STREAM_NAME')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        self.s3_prefix = os.getenv('S3_PREFIX')
        self.poll_interval = int(os.getenv('POLL_INTERVAL', '3'))
        self.region = os.getenv('AWS_DEFAULT_REGION')
        self.output_format = os.getenv('OUTPUT_FORMAT')
        
        # S3 batching configuration for 500KB Parquet files
        self.s3_batch_buffer = []
        self.batch_size = int(os.getenv('S3_BATCH_SIZE', '250'))  # ~500KB Parquet files
        self.batch_timeout = int(os.getenv('S3_BATCH_TIMEOUT', '300'))  # 5 minutes
        self.last_s3_write = time.time()
        
        # Empty poll tracking
        self.empty_poll_counter = 0
        
        # Initialize AWS clients
        try:
            self.firehose = boto3.client('firehose', region_name=self.region)
            logger.info(f"Successfully initialized Firehose client for region: {self.region}")
        except Exception as e:
            logger.error(f"Failed to initialize Firehose client: {e}")
            raise
        
        try:
            self.s3 = boto3.client('s3', region_name=self.region)
            logger.info(f"Successfully initialized S3 client for region: {self.region}")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise
        
        logger.info(f"CRM Producer initialized - Stream: {self.stream_name}, S3 Bucket: {self.s3_bucket}, S3 Prefix: {self.s3_prefix}")
        logger.info(f"S3 batching enabled: {self.batch_size} records or {self.batch_timeout}s timeout")
        logger.info(f"Output format: {self.output_format}")

    def start_health_server(self):
        """Start health check server on port 8080"""
        try:
            server = HTTPServer(('0.0.0.0', 8080), HealthHandler)
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            logger.info("Health check server started successfully on port 8080")
            
            time.sleep(1)
            try:
                import urllib.request
                response = urllib.request.urlopen('http://localhost:8080/health', timeout=5)
                if response.getcode() == 200:
                    logger.info("Health endpoint verified - responding correctly")
                else:
                    logger.warning(f"Health endpoint returned status: {response.getcode()}")
            except Exception as e:
                logger.warning(f"Could not verify health endpoint: {e}")
            
            return server
        except Exception as e:
            logger.error(f"Failed to start health server: {e}")
            raise

    def exponential_backoff(self, attempt: int, max_delay: int = 60) -> float:
        """Calculate exponential backoff delay"""
        base_delay = 2 ** attempt
        jitter = random.uniform(0.1, 0.5)
        delay = min(base_delay + jitter, max_delay)
        return delay

    def fetch_data_with_retry(self, max_retries: int = 3) -> Optional[List[Dict]]:
        """Fetch data from CRM API with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Polling CRM API: {self.api_url} (attempt {attempt + 1}/{max_retries})")
                response = requests.get(self.api_url, timeout=30)
                response.raise_for_status()
                
                # Parse JSON response
                json_response = response.json()
                
                # Check if API returned an error
                if isinstance(json_response, dict) and 'error' in json_response:
                    logger.warning(f"API returned error: {json_response['error']}")
                    return None
                
                # Handle different response formats
                if isinstance(json_response, list):
                    data = json_response
                elif isinstance(json_response, dict):
                    data = [json_response]
                else:
                    logger.error(f"Unexpected response format: {type(json_response)}")
                    return None
                
                logger.info(f"Successfully fetched {len(data)} records from CRM API")
                
                # Log sample record for debugging
                if data and len(data) > 0:
                    logger.info(f"Sample record: {data[0]}")
                
                return data
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    backoff_delay = self.exponential_backoff(attempt)
                    logger.info(f"Retrying in {backoff_delay:.2f} seconds...")
                    time.sleep(backoff_delay)
                else:
                    logger.error("Max retries exceeded for API request")
                    return None
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error fetching data: {e}")
                return None
        
        return None

    def is_valid_record(self, record: Dict) -> bool:
        """Validate CRM record against expected schema"""
        if not self.EXPECTED_FIELDS.issubset(record.keys()):
            return False
        
        # Additional CRM-specific validation
        try:
            # customer_id should be integer
            if not isinstance(record.get('customer_id'), int):
                return False
            
            # interaction_type should be non-empty string
            if not isinstance(record.get('interaction_type'), str) or not record.get('interaction_type').strip():
                return False
            
            # timestamp should be numeric
            if not isinstance(record.get('timestamp'), (int, float)):
                return False
            
            # rating should be 1-5 if present
            if 'rating' in record and record['rating'] is not None:
                if not isinstance(record['rating'], int) or not (1 <= record['rating'] <= 5):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating record: {e}")
            return False

    def validate_records(self, records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Separate valid and invalid CRM records"""
        valid_records = []
        invalid_records = []
        
        for record in records:
            if self.is_valid_record(record):
                valid_records.append(record)
            else:
                invalid_records.append(record)
                logger.warning(f"Invalid CRM record: {record}")
        
        logger.info(f"CRM validation results: {len(valid_records)} valid, {len(invalid_records)} invalid")
        return valid_records, invalid_records

    def send_to_dlq(self, records: List[Dict], reason: str = "validation_failed") -> bool:
        """Send invalid/failed records to Dead Letter Queue in S3"""
        if not records:
            return True
            
        try:
            current_date = datetime.utcnow().strftime('%Y-%m-%d')
            timestamp = int(time.time() * 1000)
            dlq_key = f"dlq/crm/{reason}/{current_date}/dlq_{timestamp}.json.gz"
            
            # Prepare DLQ data with metadata
            dlq_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "reason": reason,
                "service": "crm-producer",
                "failed_records": records
            }
            
            data_content = json.dumps(dlq_data, indent=2)
            
            # Compress DLQ data
            import gzip
            compressed_buffer = BytesIO()
            with gzip.GzipFile(mode='w', fileobj=compressed_buffer) as gz:
                gz.write(data_content.encode())
            
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=dlq_key,
                Body=compressed_buffer.getvalue(),
                ContentEncoding='gzip',
                ContentType='application/json'
            )
            
            logger.info(f"Sent {len(records)} CRM records to DLQ: s3://{self.s3_bucket}/{dlq_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send CRM records to DLQ: {e}")
            return False

    def add_to_s3_batch(self, records: List[Dict]) -> bool:
        """Add records to S3 batch buffer"""
        try:
            self.s3_batch_buffer.extend(records)
            logger.debug(f"Added {len(records)} CRM records to S3 batch buffer (total: {len(self.s3_batch_buffer)})")
            
            # Check if we should flush the batch
            should_flush = (
                len(self.s3_batch_buffer) >= self.batch_size or
                time.time() - self.last_s3_write >= self.batch_timeout
            )
            
            if should_flush:
                return self.flush_s3_batch()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to add CRM records to S3 batch: {e}")
            return False

    def flush_s3_batch(self) -> bool:
        """Flush S3 batch buffer to Parquet format"""
        if not self.s3_batch_buffer:
            return True
            
        try:
            # Create S3 key with new path structure and partitioning
            current_date = datetime.utcnow()
            timestamp = int(time.time() * 1000)
            
            # New S3 key structure: raw-data/crm-interaction-streams/landing-zone/year=2025/month=07/day=18/batch_timestamp.parquet
            s3_key = f"{self.s3_prefix}year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}/batch_{timestamp}.parquet"
            
            # Convert to DataFrame and then to Parquet
            df = pd.DataFrame(self.s3_batch_buffer)
            
            # Create Parquet buffer
            parquet_buffer = BytesIO()
            df.to_parquet(
                parquet_buffer,
                compression='snappy',  # Optimal for query performance
                index=False,
                engine='pyarrow'
            )
            
            # Get file size for logging
            file_size_kb = len(parquet_buffer.getvalue()) / 1024
            
            logger.info(f"Flushing CRM S3 batch: {len(self.s3_batch_buffer)} records to s3://{self.s3_bucket}/{s3_key} ({file_size_kb:.1f}KB)")
            
            # Upload Parquet to S3
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            logger.info(f"Successfully flushed {len(self.s3_batch_buffer)} CRM records to S3 as Parquet ({file_size_kb:.1f}KB)")
            
            # Clear batch buffer and update timestamp
            self.s3_batch_buffer.clear()
            self.last_s3_write = time.time()
            
            return True
            
        except Exception as e:
            logger.error(f"CRM S3 batch flush failed: {e}")
            return False

    def send_to_firehose_with_retry(self, records: List[Dict], max_retries: int = 3) -> bool:
        """Send records to Kinesis Firehose with retry logic"""
        if not records:
            logger.info("No CRM records to send to Firehose")
            return True
        
        for attempt in range(max_retries):
            try:
                # Create Firehose records
                firehose_records = []
                for record in records:
                    try:
                        firehose_record = {
                            'Data': json.dumps(record) + '\n'
                        }
                        firehose_records.append(firehose_record)
                    except Exception as e:
                        logger.error(f"Error preparing CRM record for Firehose: {e}")
                        continue
                
                if not firehose_records:
                    logger.info("No valid CRM records to send to Firehose after preparation")
                    return True
                
                logger.info(f"Sending {len(firehose_records)} CRM records to Firehose stream: {self.stream_name} (attempt {attempt + 1}/{max_retries})")
                
                response = self.firehose.put_record_batch(
                    DeliveryStreamName=self.stream_name,
                    Records=firehose_records
                )
                
                failed_count = response.get('FailedPutCount', 0)
                if failed_count > 0:
                    logger.warning(f"Failed to send {failed_count} CRM records to Firehose")
                    
                    # Extract failed records for DLQ
                    failed_records = []
                    for i, record_result in enumerate(response.get('RequestResponses', [])):
                        if 'ErrorCode' in record_result:
                            logger.error(f"CRM record {i} failed: {record_result}")
                            if i < len(records):
                                failed_records.append(records[i])
                    
                    # Send failed records to DLQ
                    if failed_records:
                        self.send_to_dlq(failed_records, "firehose_failed")
                    
                    return False
                
                logger.info(f"Successfully sent {len(firehose_records)} CRM records to Firehose")
                return True
                
            except Exception as e:
                logger.error(f"CRM Firehose send failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    backoff_delay = self.exponential_backoff(attempt)
                    logger.info(f"Retrying CRM Firehose send in {backoff_delay:.2f} seconds...")
                    time.sleep(backoff_delay)
                else:
                    logger.error("Max retries exceeded for CRM Firehose send")
                    # Send to DLQ as final fallback
                    self.send_to_dlq(records, "firehose_max_retries_exceeded")
                    return False
        
        return False

    def send_to_both_destinations(self, records: List[Dict]) -> Tuple[bool, bool]:
        """Send CRM data to both S3 (batched Parquet) and Firehose (immediate)"""
        # Firehose: Send immediately for real-time processing
        firehose_success = self.send_to_firehose_with_retry(records)
        
        # S3: Add to batch buffer for cost-optimized Parquet storage
        s3_success = self.add_to_s3_batch(records)
        
        return s3_success, firehose_success

    def alert_no_data(self):
        """Alert when no CRM data has been received for extended period"""
        logger.error("ALERT: No CRM data received in the last 10 polling cycles!")
        # Optional: Add SNS notification, CloudWatch metric, or Slack webhook here
        
        # Example CloudWatch metric (optional)
        try:
            cloudwatch = boto3.client('cloudwatch', region_name=self.region)
            cloudwatch.put_metric_data(
                Namespace='DataPipeline/CRMProducer',
                MetricData=[
                    {
                        'MetricName': 'NoDataAlert',
                        'Value': 1,
                        'Unit': 'Count',
                        'Timestamp': datetime.utcnow()
                    }
                ]
            )
            logger.info("Published CRM no-data alert metric to CloudWatch")
        except Exception as e:
            logger.error(f"Failed to publish CRM CloudWatch metric: {e}")

    def run_cycle(self):
        """Run one polling cycle with enhanced CRM features"""
        try:
            # Fetch data with retry logic
            data = self.fetch_data_with_retry()
            
            if data:
                # RESET COUNTER: Data arrived, reset empty poll tracking
                self.empty_poll_counter = 0
                
                # Validate records
                valid_records, invalid_records = self.validate_records(data)
                
                # Send invalid records to DLQ
                if invalid_records:
                    self.send_to_dlq(invalid_records, "validation_failed")
                
                if valid_records:
                    logger.info(f"Processing {len(valid_records)} valid CRM records")
                    
                    # Send to both destinations
                    s3_success, firehose_success = self.send_to_both_destinations(valid_records)
                    
                    # Log results
                    if s3_success and firehose_success:
                        logger.info("Successfully processed CRM data to both S3 Parquet batch and Firehose")
                    elif s3_success:
                        logger.warning("S3 Parquet batch succeeded, Firehose failed")
                    elif firehose_success:
                        logger.warning("Firehose succeeded, S3 Parquet batch failed")
                    else:
                        logger.error("Both S3 Parquet batch and Firehose failed")
                else:
                    logger.warning("No valid CRM records to process after validation")
                    
            else:
                # INCREMENT COUNTER: No data, track empty poll
                self.empty_poll_counter += 1
                logger.info(f"No CRM data available in this polling cycle (empty polls: {self.empty_poll_counter})")
                
                # Log context about empty polls
                if self.empty_poll_counter % 10 == 0:
                    logger.warning(f"No CRM data received in the last {self.empty_poll_counter * self.poll_interval}s")
                
                # Trigger alert after 10 empty polls
                if self.empty_poll_counter >= 10:
                    self.alert_no_data()
                
        except Exception as e:
            logger.error(f"Error in CRM polling cycle: {e}")

    def apply_backoff_logic(self):
        """Apply intelligent backoff when no CRM data is available"""
        if self.empty_poll_counter >= 5:
            backoff_time = min(self.poll_interval * 2, 300)  # Max 5 minutes
            logger.info(f"Backing off for {backoff_time}s due to {self.empty_poll_counter} consecutive empty CRM polls")
            time.sleep(backoff_time)

    def run(self):
        """Main run loop with enhanced CRM features"""
        logger.info("Starting Enhanced CRM Producer with:")
        logger.info("- Dual streaming (S3 Parquet batching + Firehose real-time)")
        logger.info("- Retry logic with exponential backoff")
        logger.info("- CRM data validation and DLQ support")
        logger.info("- Smart polling with adaptive backoff")
        logger.info("- Parquet format for cost optimization")
        logger.info("- Target file size: ~500KB")
        logger.info("- NO DATA ENRICHMENT")
        
        try:
            health_server = self.start_health_server()
            logger.info("Health server started successfully")
        except Exception as e:
            logger.error(f"Failed to start health server: {e}")
            return
        
        time.sleep(2)
        
        try:
            logger.info(f"Starting CRM main polling loop with {self.poll_interval}s base interval")
            cycle_count = 0
            
            while True:
                cycle_count += 1
                logger.info(f"Starting CRM polling cycle #{cycle_count}")
                
                # Run the polling cycle
                self.run_cycle()
                
                # Apply backoff logic if needed
                self.apply_backoff_logic()
                
                # Flush S3 batch if timeout reached
                if time.time() - self.last_s3_write >= self.batch_timeout:
                    self.flush_s3_batch()
                
                logger.info(f"Completed CRM polling cycle #{cycle_count}, sleeping for {self.poll_interval}s")
                time.sleep(self.poll_interval)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, flushing final CRM S3 batch...")
            self.flush_s3_batch()
            logger.info("Shutting down Enhanced CRM Producer")
        except Exception as e:
            logger.error(f"Unexpected error in CRM main loop: {e}")
            # Flush batch before exiting
            self.flush_s3_batch()
            raise

if __name__ == "__main__":
    try:
        producer = CRMProducer()
        producer.run()
    except Exception as e:
        logger.error(f"Fatal error starting Enhanced CRM Producer: {e}")
        exit(1)
