with t1 as (
    select * from {{ref('Sales_January_2019')}}
    where OrderID is not null 
        and OrderID != 'OrderID' 
        and OrderID != 'Order ID'
),

t2 as (
    select * from {{ref('Sales_February_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t3 as (
    select * from {{ref('Sales_March_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t4 as (
    select * from {{ref('Sales_April_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t5 as (
    select * from {{ref('Sales_May_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t6 as (
    select * from {{ref('Sales_June_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t7 as (
    select * from {{ref('Sales_July_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t8 as (
    select * from {{ref('Sales_August_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t9 as (
    select * from {{ref('Sales_September_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t10 as (
    select * from {{ref('Sales_October_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t11 as (
    select * from {{ref('Sales_November_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

t12 as (
    select * from {{ref('Sales_December_2019')}}
    where OrderID is not null and OrderID != 'OrderID'
        and OrderID != 'Order ID'
),

final as(
    select * from t1
    union all
    select * from t2
    union all
    select * from t3
    union all
    select * from t4
    union all
    select * from t5
    union all
    select * from t6
    union all
    select * from t7
    union all
    select * from t8
    union all
    select * from t9
    union all
    select * from t10
    union all
    select * from t11
    union all
    select * from t12
)



SELECT ROW_NUMBER() OVER (ORDER BY OrderDate ASC) AS order_detail_id, *
FROM final



