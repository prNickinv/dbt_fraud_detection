WITH source AS (
    -- Read from ClickHouse transactions source table
    SELECT * FROM {{ source('clickhouse_stream', 'transactions') }}
),

renamed AS (
    SELECT
        -- Generate a surrogate key for unique identification
        {{ dbt_utils.generate_surrogate_key([
            'transaction_time', 'merch', 'amount', 'name_1', 'name_2'
        ]) }} AS transaction_id,

        -- Date & Time
        transaction_time AS transaction_dt, -- Original timestamp
        toDate(transaction_time) AS transaction_date, -- Date part only

        -- Merchant Info
        merch AS merchant_name,
        cat_id AS category,

        -- Customers Info
        name_1 AS first_name,
        name_2 AS last_name,
        gender,
        jobs AS customer_job,

        -- Geographic Info
        us_state,
        one_city AS city,
        street,
        post_code,
        lat AS customer_lat,
        lon AS customer_lon,

        merchant_lat,
        merchant_lon,

        population_city,

        -- Transaction Info
        amount,
        -- Macro calculation
        {{ get_amount_bucket('amount') }} AS amount_segment,

        -- Fraud Label
        target AS is_fraud

    FROM source
)

SELECT * FROM renamed
