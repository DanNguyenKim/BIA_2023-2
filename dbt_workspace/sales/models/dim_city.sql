WITH source_data AS (
    SELECT DISTINCT PurchaseAddress
    FROM {{ ref('order_raw') }}
), 

transform_city AS (
    SELECT 
        SPLIT_PART(PurchaseAddress, ', ', 2) AS city, 
        SPLIT_PART(SPLIT_PART(PurchaseAddress, ', ', 3), ' ', 1) AS state_name,
        SPLIT_PART(SPLIT_PART(PurchaseAddress, ', ', 3), ' ', 2) AS zipcode
    FROM source_data
),

match_dim_state AS (
    SELECT a.city,
        b.state_key,
        a.zipcode
    FROM transform_city a 
    JOIN {{ ref('dim_state') }} b
        ON a.state_name = b.state_name
),

clean_city AS (
    SELECT DISTINCT * 
    FROM match_dim_state
) 

SELECT 
    ROW_NUMBER() OVER (ORDER BY zipcode) AS city_key,
    state_key,
    city,
    zipcode
FROM clean_city
