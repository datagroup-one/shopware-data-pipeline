-- =====================================================
-- POS TABLE DDL
-- =====================================================

DROP TABLE IF EXISTS public.processed_pos;

CREATE TABLE public.processed_pos (
    -- Primary identifiers
    transaction_id VARCHAR(36) NOT NULL,
    store_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    
    -- Transaction details
    quantity INTEGER NOT NULL,
    revenue DECIMAL(10,2) NOT NULL,
    discount_applied DECIMAL(10,2) DEFAULT 0.00,
    gross_revenue DECIMAL(10,2) NOT NULL,
    net_revenue DECIMAL(10,2) NOT NULL,
    discount_percentage DECIMAL(5,2) DEFAULT 0.00,
    revenue_per_item DECIMAL(10,2) NOT NULL,
    has_discount BOOLEAN DEFAULT FALSE,
    
    -- Temporal columns
    transaction_datetime TIMESTAMP NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_year INTEGER NOT NULL,
    transaction_month INTEGER NOT NULL,
    transaction_day INTEGER NOT NULL,
    transaction_hour INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    year_month VARCHAR(6) NOT NULL,
    
    -- Categorization columns
    transaction_size_category VARCHAR(20) NOT NULL,
    quantity_category VARCHAR(20) NOT NULL,
    
    -- Analytics columns
    running_total_revenue DECIMAL(12,2),
    transaction_rank_in_store INTEGER,
    daily_store_transaction_count INTEGER,
    daily_store_revenue DECIMAL(12,2),
    daily_product_sales INTEGER,
    daily_product_revenue DECIMAL(12,2),
    
    -- Data quality flags
    is_valid_transaction BOOLEAN DEFAULT TRUE,
    
    -- Metadata columns
    processing_timestamp TIMESTAMP NOT NULL,
    job_run_id VARCHAR(100) NOT NULL,
    
    -- Primary key
    PRIMARY KEY (transaction_id)
)
DISTKEY (store_id)
SORTKEY (transaction_date, store_id, transaction_datetime)
;

-- Create indexes for better query performance
CREATE INDEX idx_transactions_store_date ON public.processed_pos (store_id, transaction_date);
CREATE INDEX idx_transactions_product_date ON public.processed_pos (product_id, transaction_date);
CREATE INDEX idx_transactions_datetime ON public.processed_pos (transaction_datetime);
CREATE INDEX idx_transactions_year_month ON public.processed_pos (year_month);
CREATE INDEX idx_transactions_size_category ON public.processed_pos (transaction_size_category);

-- Add column comments for documentation
COMMENT ON TABLE public.processed_pos IS 'Processed transaction data with analytics-ready transformations';
COMMENT ON COLUMN public.processed_pos.transaction_id IS 'Unique identifier for each transaction';
COMMENT ON COLUMN public.processed_pos.gross_revenue IS 'Revenue before discount applied';
COMMENT ON COLUMN public.processed_pos.net_revenue IS 'Revenue after discount applied';
COMMENT ON COLUMN public.processed_pos.discount_percentage IS 'Discount as percentage of gross revenue';
COMMENT ON COLUMN public.processed_pos.running_total_revenue IS 'Cumulative revenue per store';
COMMENT ON COLUMN public.processed_pos.year_month IS 'YYYYMM format for partitioning';
