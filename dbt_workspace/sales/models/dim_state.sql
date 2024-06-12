WITH source_data AS (
    SELECT DISTINCT PurchaseAddress
    FROM {{ ref('order_raw') }}
), 

transform_state1 AS (
    SELECT SPLIT_PART(PurchaseAddress, ', ', 3) AS state_name1
    FROM source_data
),

transform_state2 AS (
    SELECT DISTINCT SPLIT_PART(state_name1, ' ', 1) AS state_name
    FROM transform_state1
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY state_name) AS state_key,
    state_name
FROM transform_state2