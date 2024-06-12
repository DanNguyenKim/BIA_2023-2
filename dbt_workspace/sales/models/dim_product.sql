with source_data as (
    select distinct Product as product_name,
        cast(PriceEach as FLOAT) as price,
    from {{ ref('order_raw')}}
)

select 
    ROW_NUMBER() OVER (ORDER BY product_name) AS product_key,
    * 
from source_data