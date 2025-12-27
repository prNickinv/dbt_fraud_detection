-- There should be no negative amounts in the staging transactions table

SELECT *
FROM {{ ref('stg_transactions') }}
WHERE amount < 0
