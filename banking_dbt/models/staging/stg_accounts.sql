{{ config(
    materialized='snapshot',
    unique_key='account_id',
    strategy='timestamp',
    updated_at='created_at'
) }}

WITH ranked AS (
    SELECT
        {{ dbt_utils.star(from=ref('stg_accounts')) }},
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY created_at DESC
        ) AS rn
    FROM {{ ref('stg_accounts') }}
)

SELECT
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
FROM ranked
WHERE rn = 1
