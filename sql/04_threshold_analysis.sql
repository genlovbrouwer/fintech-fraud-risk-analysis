-- Phase 5 — Threshold and Review Logic
-- SQL dialect: DuckDB
--
-- Goal:
-- 1. Define candidate review thresholds
-- 2. Estimate fraud captured above each threshold
-- 3. Estimate legitimate volume affected
-- 4. Recommend a practical review strategy
--
-- Assumes this view already exists:
-- CREATE OR REPLACE VIEW paysim AS
-- SELECT * FROM read_csv_auto('data/raw/fintech_fraud_trans.csv');

------------------------------------------------------------
-- 0. Base totals for reference
------------------------------------------------------------

SELECT
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_value,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS total_fraud_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS total_fraud_value,
    SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS total_legit_count,
    SUM(CASE WHEN isFraud = 0 THEN amount ELSE 0 END) AS total_legit_value
FROM paysim;

------------------------------------------------------------
-- 1. Candidate thresholds
--
-- Threshold_A:
--   Review all transactions above 200000
--
-- Threshold_B:
--   Review TRANSFER and CASH_OUT transactions above 200000
------------------------------------------------------------

WITH threshold_results AS (
    SELECT
        'Threshold_A: amount > 200K' AS threshold_name,
        COUNT(*) AS flagged_transactions,
        SUM(amount) AS flagged_value,
        SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_captured_count,
        SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_captured_value,
        SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS legit_affected_count,
        SUM(CASE WHEN isFraud = 0 THEN amount ELSE 0 END) AS legit_affected_value
    FROM paysim
    WHERE amount > 200000

    UNION ALL

    SELECT
        'Threshold_B: TRANSFER/CASH_OUT and amount > 200K' AS threshold_name,
        COUNT(*) AS flagged_transactions,
        SUM(amount) AS flagged_value,
        SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_captured_count,
        SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_captured_value,
        SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS legit_affected_count,
        SUM(CASE WHEN isFraud = 0 THEN amount ELSE 0 END) AS legit_affected_value
    FROM paysim
    WHERE type IN ('TRANSFER', 'CASH_OUT')
      AND amount > 200000
),
base_totals AS (
    SELECT
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_value,
        SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS total_fraud_count,
        SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS total_fraud_value,
        SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS total_legit_count,
        SUM(CASE WHEN isFraud = 0 THEN amount ELSE 0 END) AS total_legit_value
    FROM paysim
)
SELECT
    t.threshold_name,
    t.flagged_transactions,
    t.flagged_value,
    t.fraud_captured_count,
    t.fraud_captured_value,
    ROUND(100.0 * t.fraud_captured_count / NULLIF(b.total_fraud_count, 0), 2) AS fraud_count_capture_pct,
    ROUND(100.0 * t.fraud_captured_value / NULLIF(b.total_fraud_value, 0), 2) AS fraud_value_capture_pct,
    t.legit_affected_count,
    t.legit_affected_value,
    ROUND(100.0 * t.legit_affected_count / NULLIF(b.total_legit_count, 0), 2) AS legit_count_affected_pct,
    ROUND(100.0 * t.legit_affected_value / NULLIF(b.total_legit_value, 0), 2) AS legit_value_affected_pct,
    ROUND(100.0 * t.flagged_transactions / NULLIF(b.total_transactions, 0), 2) AS total_txns_flagged_pct,
    ROUND(100.0 * t.flagged_value / NULLIF(b.total_value, 0), 2) AS total_value_flagged_pct
FROM threshold_results t
CROSS JOIN base_totals b
ORDER BY threshold_name;

------------------------------------------------------------
-- 2. Optional detail check for Threshold_A
------------------------------------------------------------

SELECT
    type,
    COUNT(*) AS flagged_transactions,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_captured_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_captured_value,
    SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS legit_affected_count
FROM paysim
WHERE amount > 200000
GROUP BY type
ORDER BY fraud_captured_count DESC, flagged_transactions DESC;

------------------------------------------------------------
-- 3. Optional detail check for Threshold_B
------------------------------------------------------------

SELECT
    type,
    COUNT(*) AS flagged_transactions,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_captured_count,
    SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_captured_value,
    SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS legit_affected_count
FROM paysim
WHERE type IN ('TRANSFER', 'CASH_OUT')
  AND amount > 200000
GROUP BY type
ORDER BY fraud_captured_count DESC, flagged_transactions DESC;

------------------------------------------------------------
-- 4. Optional third candidate threshold
--
-- Threshold_C:
--   Review only TRANSFER transactions above 100000
--
-- This is narrower and can be used as an extra comparison if needed.
------------------------------------------------------------

WITH threshold_c AS (
    SELECT
        'Threshold_C: TRANSFER and amount > 100K' AS threshold_name,
        COUNT(*) AS flagged_transactions,
        SUM(amount) AS flagged_value,
        SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_captured_count,
        SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_captured_value,
        SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS legit_affected_count,
        SUM(CASE WHEN isFraud = 0 THEN amount ELSE 0 END) AS legit_affected_value
    FROM paysim
    WHERE type = 'TRANSFER'
      AND amount > 100000
),
base_totals AS (
    SELECT
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_value,
        SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS total_fraud_count,
        SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS total_fraud_value,
        SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS total_legit_count,
        SUM(CASE WHEN isFraud = 0 THEN amount ELSE 0 END) AS total_legit_value
    FROM paysim
)
SELECT
    t.threshold_name,
    t.flagged_transactions,
    t.flagged_value,
    t.fraud_captured_count,
    t.fraud_captured_value,
    ROUND(100.0 * t.fraud_captured_count / NULLIF(b.total_fraud_count, 0), 2) AS fraud_count_capture_pct,
    ROUND(100.0 * t.fraud_captured_value / NULLIF(b.total_fraud_value, 0), 2) AS fraud_value_capture_pct,
    t.legit_affected_count,
    t.legit_affected_value,
    ROUND(100.0 * t.legit_affected_count / NULLIF(b.total_legit_count, 0), 2) AS legit_count_affected_pct,
    ROUND(100.0 * t.legit_affected_value / NULLIF(b.total_legit_value, 0), 2) AS legit_value_affected_pct,
    ROUND(100.0 * t.flagged_transactions / NULLIF(b.total_transactions, 0), 2) AS total_txns_flagged_pct,
    ROUND(100.0 * t.flagged_value / NULLIF(b.total_value, 0), 2) AS total_value_flagged_pct
FROM threshold_c t
CROSS JOIN base_totals b;