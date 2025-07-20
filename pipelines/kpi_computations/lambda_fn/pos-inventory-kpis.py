import boto3
import os
import json
import time

# Redshift Data API client
redshift = boto3.client('redshift-data')

# Load from environment
DB_NAME = os.environ['DB_NAME']
REDSHIFT_WORKGROUP = os.environ['REDSHIFT_WORKGROUP']
SECRET_ARN = os.environ['REDSHIFT_SECRET_ARN']

MAX_RETRIES = 3
RETRY_DELAY = 3

def log(msg):
    print(f"[KPI-LAMBDA] {msg}")

def execute_sql_with_retry(sql, description):
    """Run Redshift SQL with retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log(f"Running [{description}] (attempt {attempt})")
            response = redshift.execute_statement(
                Database=DB_NAME,
                SecretArn=SECRET_ARN,
                WorkgroupName=REDSHIFT_WORKGROUP,
                Sql=sql
            )
            statement_id = response['Id']

            # Wait for completion
            while True:
                status = redshift.describe_statement(Id=statement_id)
                if status['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                time.sleep(1)

            if status['Status'] != 'FINISHED':
                raise Exception(f"Query [{description}] failed: {status.get('Error')}")

            log(f"Success: [{description}]")
            return
        except Exception as e:
            log(f"Error in [{description}]: {e}")
            if attempt < MAX_RETRIES:
                log(f"Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                raise

def lambda_handler(event, context):
    try:
        log("KPI Lambda triggered.")

        kpi_views = {
            # Sales KPIs
            "sales_kpi_by_region_product": """
                CREATE OR REPLACE VIEW sales_kpi_by_region_product AS
                SELECT 
                    product_id,
                    store_id,
                    SUM(net_revenue) AS total_net_revenue,
                    SUM(quantity) AS total_units_sold,
                    COUNT(DISTINCT transaction_id) AS transaction_count,
                    AVG(revenue_per_item) AS avg_revenue_per_item
                FROM processed_pos 
                GROUP BY product_id, store_id;
            """,
            "stock_availability": """
                CREATE OR REPLACE VIEW stock_availability AS
                SELECT 
                    i.product_id,
                    i.warehouse_id,
                    i.stock_level,
                    i.stock_status,
                    i.needs_restock,
                    i.stock_level_category
                FROM processed_inventory i
                WHERE i.is_valid_inventory = true;
            """,
            "turnover_rate": """
                CREATE OR REPLACE VIEW turnover_rate AS
                SELECT 
                    p.product_id,
                    SUM(p.quantity) AS total_sold,
                    AVG(i.product_total_stock_all_warehouses) AS avg_total_stock,
                    ROUND(SUM(p.quantity) / NULLIF(AVG(i.product_total_stock_all_warehouses), 0), 2) AS turnover_rate
                FROM processed_pos p
                JOIN processed_inventory i ON p.product_id = i.product_id
                WHERE p.is_valid_transaction = true AND i.is_valid_inventory = true
                GROUP BY p.product_id;
            """,

            # Operations KPIs
            "inventory_turnover": """
                CREATE OR REPLACE VIEW inventory_turnover AS
                SELECT 
                    i.product_id,
                    i.warehouse_id,
                    SUM(p.quantity) AS total_units_sold,
                    AVG(i.stock_level) AS avg_stock_level,
                    SUM(p.quantity) / NULLIF(AVG(i.stock_level), 0) AS turnover
                FROM processed_pos p
                JOIN processed_inventory i ON p.product_id = i.product_id
                WHERE p.is_valid_transaction = true AND i.is_valid_inventory = true
                GROUP BY i.product_id, i.warehouse_id;
            """,
            "restocking_frequency": """
                CREATE OR REPLACE VIEW restocking_frequency AS
                SELECT 
                    product_id,
                    warehouse_id,
                    SUM(CASE WHEN needs_restock = true THEN 1 ELSE 0 END) AS restock_events,
                    COUNT(*) AS total_events,
                    ROUND(
                        SUM(CASE WHEN needs_restock = true THEN 1 ELSE 0 END)::decimal
                        / NULLIF(COUNT(*), 0),
                        2
                    ) AS restock_rate
                FROM processed_inventory
                WHERE is_valid_inventory = true
                GROUP BY product_id, warehouse_id;
            """,
            "stockout_alerts": """
                CREATE OR REPLACE VIEW stockout_alerts AS
                SELECT 
                    product_id,
                    warehouse_id,
                    stock_level,
                    restock_threshold,
                    stock_status,
                    inventory_risk_level,
                    needs_restock
                FROM processed_inventory
                WHERE is_valid_inventory = true AND stock_status = 'Low Stock';
            """
        }

        for view_name, sql in kpi_views.items():
            execute_sql_with_retry(sql, view_name)

        log("All KPI views created/refreshed successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps('KPI views refreshed.')
        }

    except Exception as e:
        log(f"Lambda failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'KPI failure: {str(e)}')
        }
