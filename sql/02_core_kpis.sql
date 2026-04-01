CREATE OR REPLACE VIEW paysim AS
SELECT *
FROM read_csv_auto('data/raw/fintech_fraud_trans.csv');

------------------------------------------------------------
-- 1. Fraud rate by type
------------------------------------------------------------

SELECT
    type,
    COUNT(*) AS total_transactions,
    SUM(isFraud) AS fraud_count,
    SUM(amount) AS total_value,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim
GROUP BY type
ORDER BY fraud_rate_pct DESC;

------------------------------------------------------------
-- 2. Optional: one combined KPI table
------------------------------------------------------------

SELECT
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_value,
    SUM(isFraud) AS fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim;