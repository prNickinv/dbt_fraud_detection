-- The sum of monetary amount associated with fraud should be identical
-- whether we aggregate by state or by category

WITH state_metrics AS (
    SELECT sum(fraud_amount) AS state_total
    FROM {{ ref('mart_fraud_by_state') }}
),

category_metrics AS (
    SELECT sum(fraud_amount) AS cat_total
    FROM {{ ref('mart_fraud_by_category') }}
)

SELECT
    state_metrics.state_total,
    category_metrics.cat_total,
    abs(state_metrics.state_total - category_metrics.cat_total) AS diff
FROM state_metrics
CROSS JOIN category_metrics
-- We allow a small floating point error 0.01
WHERE abs(state_metrics.state_total - category_metrics.cat_total) > 0.01
