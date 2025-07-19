-- =====================================================
-- INVENTORY TABLE DDL
-- =====================================================

DROP TABLE IF EXISTS public.processed_inventory;

CREATE TABLE public.processed_inventory (
    -- Primary identifiers
    inventory_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    warehouse_id INTEGER NOT NULL,
    
    -- Stock information
    stock_level INTEGER NOT NULL,
    restock_threshold INTEGER,
    restock_threshold_filled INTEGER NOT NULL,
    needs_restock BOOLEAN DEFAULT FALSE,
    stock_status VARCHAR(20) NOT NULL,
    inventory_risk_level VARCHAR(20) NOT NULL,
    stock_level_category VARCHAR(20) NOT NULL,
    
    -- Calculated metrics
    stock_above_threshold INTEGER DEFAULT 0,
    stock_coverage_ratio DECIMAL(10,4) DEFAULT 0.0000,
    
    -- Temporal columns
    last_updated_datetime TIMESTAMP NOT NULL,
    last_updated_date DATE NOT NULL,
    days_since_update INTEGER NOT NULL,
    data_freshness VARCHAR(20) NOT NULL,
    update_year INTEGER NOT NULL,
    update_month INTEGER NOT NULL,
    update_day INTEGER NOT NULL,
    update_hour INTEGER NOT NULL,
    update_day_of_week INTEGER NOT NULL,
    update_week_of_year INTEGER NOT NULL,
    year_month VARCHAR(6) NOT NULL,
    
    -- Warehouse analytics
    warehouse_total_products INTEGER NOT NULL,
    warehouse_total_stock INTEGER NOT NULL,
    warehouse_avg_stock DECIMAL(10,2) NOT NULL,
    warehouse_low_stock_count INTEGER NOT NULL,
    warehouse_stock_efficiency DECIMAL(10,4) NOT NULL,
    warehouse_restock_rate DECIMAL(5,4) NOT NULL,
    
    -- Product analytics
    product_total_stock_all_warehouses INTEGER NOT NULL,
    product_warehouse_count INTEGER NOT NULL,
    product_avg_stock_per_warehouse DECIMAL(10,2) NOT NULL,
    product_min_stock_warehouse INTEGER NOT NULL,
    product_max_stock_warehouse INTEGER NOT NULL,
    
    -- Data quality flags
    is_valid_inventory BOOLEAN DEFAULT TRUE,
    has_null_restock_threshold BOOLEAN DEFAULT FALSE,
    
    -- Metadata columns
    processing_timestamp TIMESTAMP NOT NULL,
    job_run_id VARCHAR(100) NOT NULL,
    
    -- Primary key
    PRIMARY KEY (inventory_id)
)
DISTKEY (warehouse_id)
SORTKEY (warehouse_id, product_id, last_updated_date)
;

-- Create indexes for better query performance
CREATE INDEX idx_inventory_warehouse_product ON public.processed_inventory (warehouse_id, product_id);
CREATE INDEX idx_inventory_product_warehouse ON public.processed_inventory (product_id, warehouse_id);
CREATE INDEX idx_inventory_stock_status ON public.processed_inventory (stock_status);
CREATE INDEX idx_inventory_risk_level ON public.processed_inventory (inventory_risk_level);
CREATE INDEX idx_inventory_needs_restock ON public.processed_inventory (needs_restock);
CREATE INDEX idx_inventory_last_updated ON public.processed_inventory (last_updated_date);
CREATE INDEX idx_inventory_year_month ON public.processed_inventory (year_month);
CREATE INDEX idx_inventory_data_freshness ON public.processed_inventory (data_freshness);

-- Add column comments for documentation
COMMENT ON TABLE public.processed_inventory IS 'Processed inventory data with analytics-ready transformations';
COMMENT ON COLUMN public.processed_inventory.inventory_id IS 'Unique identifier for each inventory record';
COMMENT ON COLUMN public.processed_inventory.restock_threshold_filled IS 'Restock threshold with nulls imputed';
COMMENT ON COLUMN public.processed_inventory.stock_coverage_ratio IS 'Stock level divided by restock threshold';
COMMENT ON COLUMN public.processed_inventory.warehouse_stock_efficiency IS 'Average stock per product in warehouse';
COMMENT ON COLUMN public.processed_inventory.warehouse_restock_rate IS 'Percentage of products needing restock';
COMMENT ON COLUMN public.processed_inventory.data_freshness IS 'How recent the inventory data is';

