-- There should be no fraud rates outside the bounds of 0% to 100%

SELECT *
FROM {{ ref('mart_fraud_by_category') }}
WHERE
    fraud_rate_pct >= 100
    OR fraud_rate_pct <= 0
