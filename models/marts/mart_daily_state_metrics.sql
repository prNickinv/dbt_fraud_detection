WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

final AS (
    SELECT
        transaction_date,
        us_state,
        count(*) AS total_transactions,
        sum(amount) AS total_amount,
        avg(amount) AS avg_amount,
        quantile(0.95)(amount) AS p95_amount, -- noqa: PRS,AL03
        countIf(amount_segment = 'Large') / count(*) AS share_of_large_transactions

    FROM transactions
    GROUP BY
        transaction_date,
        us_state
)

SELECT * FROM final
