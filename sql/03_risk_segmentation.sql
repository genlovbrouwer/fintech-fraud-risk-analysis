-- Phase 4 — Risk Segmentation
-- SQL dialect: DuckDB
--
-- Goal:
-- 1. Analyze fraud by transaction type
-- 2. Analyze fraud by amount band
-- 3. Analyze fraud by balance band
-- 4. Identify combinations of factors associated with high risk
--
-- Assumes this view already exists:
-- CREATE OR REPLACE VIEW paysim AS
-- SELECT * FROM read_csv_auto('data/raw/fintech_fraud_trans.csv');

------------------------------------------------------------
-- 0. Create segmented view
------------------------------------------------------------

CREATE OR REPLACE VIEW paysim_segmented AS
SELECT
    *,
    CASE
        WHEN amount <= 1000 THEN '0-1K'
        WHEN amount <= 10000 THEN '1K-10K'
        WHEN amount <= 50000 THEN '10K-50K'
        WHEN amount <= 100000 THEN '50K-100K'
        WHEN amount <= 200000 THEN '100K-200K'
        ELSE '200K+'
    END AS amount_band,
    CASE
        WHEN oldbalanceOrg = 0 THEN '0'
        WHEN oldbalanceOrg <= 1000 THEN '1-1K'
        WHEN oldbalanceOrg <= 10000 THEN '1K-10K'
        WHEN oldbalanceOrg <= 100000 THEN '10K-100K'
        ELSE '100K+'
    END AS origin_balance_band,
    CASE
        WHEN oldbalanceOrg = 0 THEN 1
        ELSE 0
    END AS origin_balance_zero_flag
FROM paysim;

------------------------------------------------------------
-- 1. Fraud by transaction type
------------------------------------------------------------

SELECT
    type,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_value,
    SUM(isFraud) AS fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim_segmented
GROUP BY type
ORDER BY fraud_rate_pct DESC, fraud_value DESC;

------------------------------------------------------------
-- 2. Fraud by amount band
------------------------------------------------------------

SELECT
    amount_band,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_value,
    SUM(isFraud) AS fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim_segmented
GROUP BY amount_band
ORDER BY
    CASE amount_band
        WHEN '0-1K' THEN 1
        WHEN '1K-10K' THEN 2
        WHEN '10K-50K' THEN 3
        WHEN '50K-100K' THEN 4
        WHEN '100K-200K' THEN 5
        WHEN '200K+' THEN 6
    END;

------------------------------------------------------------
-- 3. Fraud by origin balance band
------------------------------------------------------------

SELECT
    origin_balance_band,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_value,
    SUM(isFraud) AS fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim_segmented
GROUP BY origin_balance_band
ORDER BY
    CASE origin_balance_band
        WHEN '0' THEN 1
        WHEN '1-1K' THEN 2
        WHEN '1K-10K' THEN 3
        WHEN '10K-100K' THEN 4
        WHEN '100K+' THEN 5
    END;

------------------------------------------------------------
-- 4. Simple zero-balance risk check
------------------------------------------------------------

SELECT
    origin_balance_zero_flag,
    COUNT(*) AS total_transactions,
    SUM(isFraud) AS fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim_segmented
GROUP BY origin_balance_zero_flag
ORDER BY origin_balance_zero_flag DESC;

------------------------------------------------------------
-- 5. High-risk combinations: transaction type + amount band
-- Filter out tiny segments to avoid misleading results
------------------------------------------------------------

SELECT
    type,
    amount_band,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_value,
    SUM(isFraud) AS fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim_segmented
GROUP BY type, amount_band
HAVING COUNT(*) >= 100
ORDER BY fraud_rate_pct DESC, fraud_value DESC, total_transactions DESC;

------------------------------------------------------------
-- 6. High-risk combinations: transaction type + origin balance band
------------------------------------------------------------

SELECT
    type,
    origin_balance_band,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_value,
    SUM(isFraud) AS fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim_segmented
GROUP BY type, origin_balance_band
HAVING COUNT(*) >= 100
ORDER BY fraud_rate_pct DESC, fraud_value DESC, total_transactions DESC;

------------------------------------------------------------
-- 7. High-risk combinations: transaction type + amount band + balance band
-- Use a higher minimum volume because the segments are more granular
------------------------------------------------------------

SELECT
    type,
    amount_band,
    origin_balance_band,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_value,
    SUM(isFraud) AS fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim_segmented
GROUP BY type, amount_band, origin_balance_band
HAVING COUNT(*) >= 100
ORDER BY fraud_rate_pct DESC, fraud_value DESC, total_transactions DESC;

------------------------------------------------------------
-- 8. Optional: fraud share contribution by segment
-- Useful for finding which segments contribute most to total fraud count
------------------------------------------------------------

SELECT
    type,
    amount_band,
    COUNT(*) AS total_transactions,
    SUM(isFraud) AS fraud_count,
    ROUND(
        100.0 * SUM(isFraud) / NULLIF(SUM(SUM(isFraud)) OVER (), 0),
        4
    ) AS share_of_total_fraud_pct,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim_segmented
GROUP BY type, amount_band
HAVING COUNT(*) >= 100
ORDER BY share_of_total_fraud_pct DESC, fraud_rate_pct DESC;

------------------------------------------------------------
-- 9. Optional: top high-risk segments only
-- This is useful for your dashboard table
------------------------------------------------------------

SELECT
    type,
    amount_band,
    origin_balance_band,
    COUNT(*) AS total_transactions,
    SUM(isFraud) AS fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim_segmented
GROUP BY type, amount_band, origin_balance_band
HAVING COUNT(*) >= 100
   AND SUM(isFraud) > 0
ORDER BY fraud_rate_pct DESC, fraud_value DESC
LIMIT 20;