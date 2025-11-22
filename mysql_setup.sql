-- Create database if not exists
CREATE DATABASE IF NOT EXISTS payment_scoring;
USE payment_scoring;

-- Create scored_transactions table
CREATE TABLE IF NOT EXISTS scored_transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(100) UNIQUE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    merchant_category VARCHAR(50),
    hour INT,
    day_of_week INT,
    card_present TINYINT(1),
    distance_from_home DECIMAL(10, 2),
    transaction_velocity INT,
    fraud_score DECIMAL(5, 4) NOT NULL,
    risk_level VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_transaction_id (transaction_id),
    INDEX idx_risk_level (risk_level),
    INDEX idx_timestamp (timestamp),
    INDEX idx_fraud_score (fraud_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Create a summary statistics table for monitoring
CREATE TABLE IF NOT EXISTS daily_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    total_transactions INT DEFAULT 0,
    high_risk_count INT DEFAULT 0,
    medium_risk_count INT DEFAULT 0,
    low_risk_count INT DEFAULT 0,
    avg_fraud_score DECIMAL(5, 4),
    max_amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_date (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Create a view for easy querying of high-risk transactions
CREATE OR REPLACE VIEW high_risk_transactions AS
SELECT 
    transaction_id,
    amount,
    merchant_category,
    fraud_score,
    risk_level,
    timestamp
FROM scored_transactions
WHERE risk_level = 'HIGH'
ORDER BY timestamp DESC;

-- Create a view for daily summary
CREATE OR REPLACE VIEW daily_summary AS
SELECT 
    DATE(timestamp) as transaction_date,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) as high_risk,
    SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END) as medium_risk,
    SUM(CASE WHEN risk_level = 'LOW' THEN 1 ELSE 0 END) as low_risk,
    AVG(fraud_score) as avg_fraud_score,
    MAX(amount) as max_transaction_amount,
    MIN(amount) as min_transaction_amount,
    AVG(amount) as avg_transaction_amount
FROM scored_transactions
GROUP BY DATE(timestamp)
ORDER BY transaction_date DESC;

-- Insert initial record to verify setup
INSERT INTO scored_transactions 
(transaction_id, amount, merchant_category, hour, day_of_week, card_present, 
 distance_from_home, transaction_velocity, fraud_score, risk_level)
VALUES 
('SETUP_TEST', 100.00, 'test', 12, 1, 1, 0.0, 1, 0.1234, 'LOW')
ON DUPLICATE KEY UPDATE transaction_id = transaction_id;

-- Show table structure
DESCRIBE scored_transactions;

-- Show initial data
SELECT 'Setup Complete! Sample record:' as status;
SELECT * FROM scored_transactions WHERE transaction_id = 'SETUP_TEST';