CREATE OR REPLACE VIEW paysim AS
SELECT *
FROM read_csv_auto('data/raw/fintech_fraud_trans.csv');

COPY (
    WITH base AS (
        SELECT
            COUNT(*) AS total_transactions,
            SUM(amount) AS total_transaction_value,
            SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS total_fraud_count,
            SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS total_fraud_value
        FROM paysim
    ),
    top_type AS (
        SELECT
            type AS top_risk_transaction_type
        FROM paysim
        GROUP BY type
        ORDER BY
            ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) DESC,
            SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) DESC
        LIMIT 1
    )
    SELECT
        b.total_transactions,
        b.total_transaction_value,
        b.total_fraud_count,
        b.total_fraud_value,
        ROUND(100.0 * b.total_fraud_count / b.total_transactions, 4) AS fraud_rate_pct,
        ROUND(100.0 * b.total_fraud_value / b.total_transaction_value, 4) AS fraud_value_rate_pct,
        t.top_risk_transaction_type
    FROM base b
    CROSS JOIN top_type t
) TO 'data/clean/executive_summary_dashboard.csv'
WITH (HEADER, DELIMITER ';');

COPY (
    SELECT
        step,
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_transaction_value,
        SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
        ROUND(100.0 * SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) / COUNT(*), 4) AS fraud_rate_pct
    FROM paysim
    GROUP BY step
    ORDER BY step
) TO 'data/clean/page1_fraud_trend_by_step.csv'
WITH (HEADER, DELIMITER ',');

COPY (
    WITH base AS (
        SELECT
            COUNT(*) AS total_transactions,
            SUM(amount) AS total_transaction_value,
            SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS total_fraud_count,
            SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS total_fraud_value
        FROM paysim
    ),
    top_type AS (
        SELECT
            type AS top_risk_transaction_type
        FROM paysim
        GROUP BY type
        ORDER BY
            ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) DESC,
            SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) DESC
        LIMIT 1
    )
    SELECT
        b.total_transactions,
        b.total_transaction_value,
        b.total_fraud_count,
        b.total_fraud_value,
        ROUND(100.0 * b.total_fraud_count / b.total_transactions, 4) AS fraud_rate_pct,
        ROUND(100.0 * b.total_fraud_value / b.total_transaction_value, 4) AS fraud_value_rate_pct,
        t.top_risk_transaction_type
    FROM base b
    CROSS JOIN top_type t
) TO 'data/clean/executive_summary_dashboard.txt'
WITH (HEADER, DELIMITER '\t');

COPY (
    SELECT
        type,
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_value,
        SUM(isFraud) AS fraud_count,
        SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_value,
        ROUND(100.0 * SUM(isFraud) / COUNT(*), 4) AS fraud_rate_pct
    FROM paysim
    GROUP BY type
    ORDER BY fraud_rate_pct DESC
) TO 'data/clean/page2_fraud_by_type.txt'
WITH (HEADER, DELIMITER '\t');

COPY (
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
        END
) TO 'data/clean/page2_fraud_by_amount_band.txt'
WITH (HEADER, DELIMITER '\t');

COPY (
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
    LIMIT 20
) TO 'data/clean/page2_top_risk_segments.txt'
WITH (HEADER, DELIMITER '\t');

COPY (
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
        END
) TO 'data/clean/page2_fraud_by_origin_balance_band.txt'
WITH (HEADER, DELIMITER '\t');

COPY (
    WITH threshold_results AS (
        SELECT
            'Threshold A: Amount > 200K' AS threshold_name,
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
            'Threshold B: TRANSFER/CASH_OUT > 200K' AS threshold_name,
            COUNT(*) AS flagged_transactions,
            SUM(amount) AS flagged_value,
            SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_captured_count,
            SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END) AS fraud_captured_value,
            SUM(CASE WHEN isFraud = 0 THEN 1 ELSE 0 END) AS legit_affected_count,
            SUM(CASE WHEN isFraud = 0 THEN amount ELSE 0 END) AS legit_affected_value
        FROM paysim
        WHERE type IN ('TRANSFER', 'CASH_OUT')
            AND amount > 200000

        UNION ALL

        SELECT
            'Threshold C: TRANSFER > 100K' AS threshold_name,
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
    FROM threshold_results t
    CROSS JOIN base_totals b
    ORDER BY threshold_name
) TO 'data/clean/page3_threshold_comparison.txt'
WITH (HEADER, DELIMITER '\t');