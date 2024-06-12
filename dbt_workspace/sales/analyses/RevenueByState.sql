create or replace view SALES.PUBLIC.REVENUE_BY_CITY_VIEW(
	CITY,
	TOTAL_REVENUE
) as
SELECT
    ci.city,
    SUM(so.revenue) AS total_revenue
FROM
    fact_sales_order so
JOIN
    dim_customer c ON so.customer_key = c.customer_key
JOIN 
    dim_city ci ON ci.city_key = c.city_key
GROUP BY
    c.city_key, ci.city;

SELECT * FROM SALES.PUBLIC.REVENUE_BY_CITY_VIEW;
