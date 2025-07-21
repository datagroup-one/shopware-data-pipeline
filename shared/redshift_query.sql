-- =============================================================================
-- REDSHIFT QUERIES - ORGANIZED IN CHRONOLOGICAL ORDER
-- =============================================================================

-- 1. CREATE SCHEMAS FIRST
-- =============================================================================
-- Create external schema from Glue catalog (source data)
CREATE EXTERNAL SCHEMA IF NOT EXISTS streaming_data_db 
FROM DATA CATALOG 
DATABASE 'streaming_data_db' 
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_REDSHIFT_ROLE'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Create internal schemas for views
CREATE SCHEMA IF NOT EXISTS customer_support;
CREATE SCHEMA IF NOT EXISTS marketing;

-- 2. EXPLORE SOURCE DATA (Optional - for verification)
-- =============================================================================
-- View CRM interactions data
SELECT * FROM "dev"."streaming_data_db"."crm_interactions"
LIMIT 100;

-- View web traffic logs data  
SELECT * FROM "dev"."streaming_data_db"."web_traffic_logs"
LIMIT 100;

-- 3. CREATE CUSTOMER SUPPORT VIEWS
-- =============================================================================

-- Customer Support: Feedback Scores View
CREATE OR REPLACE VIEW customer_support.feedback_scores AS 
SELECT 
    interaction_date,
    COUNT(*) AS total_rated_interactions,
    ROUND(AVG(rating), 2) AS avg_feedback_rating 
FROM streaming_data_db.crm_interactions 
WHERE has_rating = TRUE 
GROUP BY interaction_date 
WITH NO SCHEMA BINDING;

-- Customer Support: Interaction Volume View
CREATE OR REPLACE VIEW customer_support.interaction_volume AS 
SELECT 
    interaction_date,
    COUNT(*) AS total_interactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(CASE WHEN channel = 'email' THEN 1 END) AS email_interactions,
    COUNT(CASE WHEN channel = 'chat' THEN 1 END) AS chat_interactions,
    COUNT(CASE WHEN channel = 'phone' THEN 1 END) AS phone_interactions 
FROM streaming_data_db.crm_interactions 
WHERE has_channel = TRUE 
GROUP BY interaction_date 
WITH NO SCHEMA BINDING;

-- 4. CREATE MARKETING VIEWS
-- =============================================================================

-- Marketing: Engagement Scores View
CREATE OR REPLACE VIEW marketing.engagement_scores AS 
SELECT 
    event_date,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN is_engagement_event THEN 1 END) AS engagement_events,
    ROUND(100.0 * COUNT(CASE WHEN is_engagement_event THEN 1 END)::DECIMAL / COUNT(*), 2) AS engagement_score_pct 
FROM streaming_data_db.web_traffic_logs 
GROUP BY event_date 
WITH NO SCHEMA BINDING;

-- Marketing: Loyalty Activity Summary View
CREATE OR REPLACE VIEW marketing.loyalty_activity_summary AS 
SELECT 
    interaction_date,
    COUNT(*) AS total_loyalty_interactions,
    COUNT(DISTINCT customer_id) AS unique_loyalty_users,
    ROUND(AVG(rating), 2) AS avg_loyalty_rating 
FROM streaming_data_db.crm_interactions 
WHERE interaction_type = 'loyalty' 
GROUP BY interaction_date 
WITH NO SCHEMA BINDING;

-- Marketing: Session Metrics View
CREATE OR REPLACE VIEW marketing.session_metrics AS 
SELECT 
    event_date,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users,
    ROUND(AVG(path_depth), 2) AS avg_path_depth,
    SUM(CASE WHEN is_mobile THEN 1 ELSE 0 END) AS mobile_event_count 
FROM streaming_data_db.web_traffic_logs 
GROUP BY event_date 
WITH NO SCHEMA BINDING;

-- 5. VERIFY VIEWS CREATED (Optional - for testing)
-- =============================================================================

-- Test Customer Support views
SELECT * FROM customer_support.feedback_scores LIMIT 10;
SELECT * FROM customer_support.interaction_volume LIMIT 10;

-- Test Marketing views  
SELECT * FROM marketing.engagement_scores LIMIT 10;
SELECT * FROM marketing.loyalty_activity_summary LIMIT 10;
SELECT * FROM marketing.session_metrics LIMIT 10;

-- =============================================================================
-- SUMMARY OF WHAT THESE QUERIES CREATE:
-- 
-- EXTERNAL SCHEMA:
-- - streaming_data_db: Links to Glue catalog for source data access
--
-- CUSTOMER SUPPORT SCHEMA:
-- - feedback_scores: Daily feedback ratings and averages
-- - interaction_volume: Daily interaction counts by channel
--
-- MARKETING SCHEMA:  
-- - engagement_scores: Daily web engagement percentages
-- - loyalty_activity_summary: Daily loyalty program metrics
-- - session_metrics: Daily web session and user activity
--
-- NOTE: Replace 'YOUR_ACCOUNT_ID' and 'YOUR_REDSHIFT_ROLE' with actual values
-- =============================================================================