WITH source_data AS (
    SELECT DISTINCT PurchaseAddress
    FROM {{ ref('order_raw') }}
), 

transform_address AS (
    SELECT 
        PurchaseAddress,
        SPLIT_PART(PurchaseAddress, ', ', 1) AS street_address, 
        SPLIT_PART(PurchaseAddress, ', ', 2) AS city, 
        SPLIT_PART(SPLIT_PART(PurchaseAddress, ', ', 3), ' ', 2) AS zipcode
    FROM source_data
),

match_dim_city AS (
    SELECT 
        a.PurchaseAddress,
        a.street_address,
        b.city_key
    FROM transform_address a 
    JOIN {{ ref('dim_city') }} b
        ON a.city = b.city AND a.zipcode = b.zipcode
),

clean_customer AS (
    SELECT DISTINCT * 
    FROM match_dim_city
) 

SELECT 
    ROW_NUMBER() OVER (ORDER BY PurchaseAddress) AS customer_key,
    * 
FROM clean_customer