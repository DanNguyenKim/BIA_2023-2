with source_data as (
    select  *
    from {{ ref('order_raw')}}
), 
date_data as (
    select  *
    from {{ ref('dim_date')}}
),
product_data as (
    select  *
    from {{ ref('dim_product')}}
),
customer_data as (
    select  *
    from {{ ref('dim_customer')}}
),
transform_fact as (
    select 
        a.order_detail_id,
        a.OrderID,
        b.date_key,
        c.product_key,
        d.customer_key,
        cast(a.QuantityOrdered as INT) as QuantityOrdered,
        cast(a.QuantityOrdered as FLOAT)*cast(a.PriceEach as FLOAT) as revenue,
    from source_data a 
        join date_data b on a.OrderDate = b.OrderDate
        join product_data c on a.Product = c.product_name
        join customer_data d on a.PurchaseAddress = d.PurchaseAddress
)

select * 
from transform_fact