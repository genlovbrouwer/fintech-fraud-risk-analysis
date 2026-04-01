-- File path used below:
--   data/raw/fintech_fraud_trans.csv
--
-- Notes from the dataset dictionary:
-- - step = 1 hour of simulated time
-- - type = CASH-IN, CASH-OUT, DEBIT, PAYMENT, TRANSFER
-- - old/new balances represent before/after transaction balances
-- - merchant accounts often start with M and may not have destination balance info
-- - isFlaggedFraud is meant to flag transfers above 200000

------------------------------------------------------------
-- 0. Create a reusable view
------------------------------------------------------------

CREATE OR REPLACE VIEW paysim AS
SELECT *
FROM read_csv_auto('data/raw/fintech_fraud_trans.csv');

-- Quick preview
SELECT * FROM paysim LIMIT 10;

------------------------------------------------------------
-- 1. Missing values
------------------------------------------------------------

SELECT
    SUM(CASE WHEN step IS NULL THEN 1 ELSE 0 END) AS missing_step,
    SUM(CASE WHEN type IS NULL THEN 1 ELSE 0 END) AS missing_type,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS missing_amount,
    SUM(CASE WHEN nameOrig IS NULL THEN 1 ELSE 0 END) AS missing_nameOrig,
    SUM(CASE WHEN oldbalanceOrg IS NULL THEN 1 ELSE 0 END) AS missing_oldbalanceOrg,
    SUM(CASE WHEN newbalanceOrig IS NULL THEN 1 ELSE 0 END) AS missing_newbalanceOrig,
    SUM(CASE WHEN nameDest IS NULL THEN 1 ELSE 0 END) AS missing_nameDest,
    SUM(CASE WHEN oldbalanceDest IS NULL THEN 1 ELSE 0 END) AS missing_oldbalanceDest,
    SUM(CASE WHEN newbalanceDest IS NULL THEN 1 ELSE 0 END) AS missing_newbalanceDest,
    SUM(CASE WHEN isFraud IS NULL THEN 1 ELSE 0 END) AS missing_isFraud,
    SUM(CASE WHEN isFlaggedFraud IS NULL THEN 1 ELSE 0 END) AS missing_isFlaggedFraud
FROM paysim;

-- Extra check for merchant destination balances
SELECT
    COUNT(*) AS merchant_rows,
    SUM(CASE WHEN oldbalanceDest IS NULL THEN 1 ELSE 0 END) AS missing_oldbalanceDest,
    SUM(CASE WHEN newbalanceDest IS NULL THEN 1 ELSE 0 END) AS missing_newbalanceDest
FROM paysim
WHERE nameDest LIKE 'M%';

------------------------------------------------------------
-- 2. Duplicates
------------------------------------------------------------

-- Summary check
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT(
        step, '|', type, '|', amount, '|', nameOrig, '|',
        oldbalanceOrg, '|', newbalanceOrig, '|', nameDest, '|',
        oldbalanceDest, '|', newbalanceDest, '|', isFraud, '|', isFlaggedFraud
    )) AS distinct_rows,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        step, '|', type, '|', amount, '|', nameOrig, '|',
        oldbalanceOrg, '|', newbalanceOrig, '|', nameDest, '|',
        oldbalanceDest, '|', newbalanceDest, '|', isFraud, '|', isFlaggedFraud
    )) AS duplicate_rows
FROM paysim;

-- Inspect duplicate groups
SELECT
    step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
    nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud,
    COUNT(*) AS duplicate_count
FROM paysim
GROUP BY
    step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
    nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

------------------------------------------------------------
-- 3. Invalid amounts
------------------------------------------------------------

-- Negative or zero amounts
SELECT
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_amounts,
    SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) AS zero_amounts
FROM paysim;

-- Inspect invalid or suspicious amounts
SELECT *
FROM paysim
WHERE amount <= 0
ORDER BY amount;

-- Summary stats for amount
SELECT
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    AVG(amount) AS avg_amount
FROM paysim;

-- Top 20 largest transactions
SELECT *
FROM paysim
ORDER BY amount DESC
LIMIT 20;

------------------------------------------------------------
-- 4. Balance consistency checks
------------------------------------------------------------

-- 4a. Origin balance consistency
-- Expected: oldbalanceOrg - amount = newbalanceOrig

SELECT COUNT(*) AS inconsistent_origin_balance
FROM paysim
WHERE ROUND(oldbalanceOrg - amount, 2) <> ROUND(newbalanceOrig, 2);

SELECT *
FROM paysim
WHERE ROUND(oldbalanceOrg - amount, 2) <> ROUND(newbalanceOrig, 2)
LIMIT 50;

-- 4b. Destination balance consistency
-- Expected: oldbalanceDest + amount = newbalanceDest
-- Exclude merchants because the dictionary notes destination balance info may be unavailable there

SELECT COUNT(*) AS inconsistent_dest_balance
FROM paysim
WHERE nameDest NOT LIKE 'M%'
    AND ROUND(oldbalanceDest + amount, 2) <> ROUND(newbalanceDest, 2);

SELECT *
FROM paysim
WHERE nameDest NOT LIKE 'M%'
    AND ROUND(oldbalanceDest + amount, 2) <> ROUND(newbalanceDest, 2)
LIMIT 50;

-- 4c. Negative balances

SELECT
    SUM(CASE WHEN oldbalanceOrg < 0 THEN 1 ELSE 0 END) AS neg_oldbalanceOrg,
    SUM(CASE WHEN newbalanceOrig < 0 THEN 1 ELSE 0 END) AS neg_newbalanceOrig,
    SUM(CASE WHEN oldbalanceDest < 0 THEN 1 ELSE 0 END) AS neg_oldbalanceDest,
    SUM(CASE WHEN newbalanceDest < 0 THEN 1 ELSE 0 END) AS neg_newbalanceDest
FROM paysim;

------------------------------------------------------------
-- 5. Fraud distribution
------------------------------------------------------------

-- Overall fraud share
SELECT
    isFraud,
    COUNT(*) AS transactions,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct_of_total
FROM paysim
GROUP BY isFraud;

-- Fraud count by transaction type
SELECT
    type,
    isFraud,
    COUNT(*) AS transactions
FROM paysim
GROUP BY type, isFraud
ORDER BY type, isFraud;

-- Fraud rate by transaction type
SELECT
    type,
    COUNT(*) AS total_txns,
    SUM(isFraud) AS fraud_txns,
    ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
FROM paysim
GROUP BY type
ORDER BY fraud_rate_pct DESC;

------------------------------------------------------------
-- 6. Unusual values
------------------------------------------------------------

-- 6a. Very large transactions based on 3 standard deviations above average
SELECT *
FROM paysim
WHERE amount > (
    SELECT AVG(amount) + 3 * STDDEV_SAMP(amount)
    FROM paysim
)
ORDER BY amount DESC;

-- 6b. Fraud flag checks
-- Dataset rule: isFlaggedFraud is intended for transfers above 200000

SELECT *
FROM paysim
WHERE isFlaggedFraud = 1;

SELECT
    COUNT(*) AS flagged_rows,
    MIN(amount) AS min_flagged_amount,
    MAX(amount) AS max_flagged_amount
FROM paysim
WHERE isFlaggedFraud = 1;

SELECT
    type,
    COUNT(*) AS flagged_count
FROM paysim
WHERE isFlaggedFraud = 1
GROUP BY type;

-- Transfers above 200000 that were not flagged
SELECT *
FROM paysim
WHERE type = 'TRANSFER'
    AND amount > 200000
    AND isFlaggedFraud = 0;

-- Fraudulent transfers above 200000 that were not flagged
SELECT *
FROM paysim
WHERE type = 'TRANSFER'
    AND amount > 200000
    AND isFlaggedFraud = 0
    AND isFraud = 1;

-- 6c. Zero balances with non-zero transactions

SELECT *
FROM paysim
WHERE amount > 0
    AND oldbalanceOrg = 0
ORDER BY amount DESC
LIMIT 50;

SELECT *
FROM paysim
WHERE amount > 0
    AND oldbalanceDest = 0
    AND nameDest NOT LIKE 'M%'
LIMIT 50;

