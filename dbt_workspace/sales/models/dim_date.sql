WITH source_data AS (
    SELECT DISTINCT OrderDate
    FROM {{ ref('order_raw') }}
), 

transform_date AS (
    SELECT OrderDate,
        SPLIT_PART(SPLIT_PART(OrderDate, ' ', 1), '/', 1) AS month_no,
        SPLIT_PART(SPLIT_PART(OrderDate, ' ', 1), '/', 2) AS day_no,
        CONCAT('20', SPLIT_PART(SPLIT_PART(OrderDate, ' ', 1), '/', 3)) AS year_no,
        SPLIT_PART(SPLIT_PART(OrderDate, ' ', 2), ':', 1) AS hour,
        EXTRACT(DAYOFWEEK FROM TO_DATE(CONCAT(
            SPLIT_PART(SPLIT_PART(OrderDate, ' ', 1), '/', 1), '/', 
            SPLIT_PART(SPLIT_PART(OrderDate, ' ', 1), '/', 2), '/', 
            CONCAT('20', SPLIT_PART(SPLIT_PART(OrderDate, ' ', 1), '/', 3))
        ), 'MM/DD/YYYY')) AS week_day_no
    FROM source_data
),

transform_weekdays AS (
    SELECT OrderDate,
        month_no,
        day_no,
        year_no,
        hour,
        week_day_no,
        CASE 
            WHEN week_day_no = 1 THEN 'Sunday'
            WHEN week_day_no = 2 THEN 'Monday'
            WHEN week_day_no = 3 THEN 'Tuesday'
            WHEN week_day_no = 4 THEN 'Wednesday'
            WHEN week_day_no = 5 THEN 'Thursday'
            WHEN week_day_no = 6 THEN 'Friday'
            WHEN week_day_no = 7 THEN 'Saturday'
        END AS weekday_name,
        CASE 
            WHEN week_day_no = 1 THEN 'Y'
            WHEN week_day_no = 7 THEN 'Y'
            ELSE 'N'
        END AS is_weekend
    FROM transform_date
)
-- weekday chủ nhật: 1 -> thứ 7: 7
SELECT 
    CONCAT(
        SPLIT_PART(SPLIT_PART(OrderDate, ' ', 1), '/', 2), 
        SPLIT_PART(SPLIT_PART(OrderDate, ' ', 1), '/', 1), 
        CONCAT('20', SPLIT_PART(SPLIT_PART(OrderDate, ' ', 1), '/', 3)), 
        SPLIT_PART(SPLIT_PART(OrderDate, ' ', 2), ':', 1),  
        SPLIT_PART(SPLIT_PART(OrderDate, ' ', 2), ':', 2)
    ) AS date_key,
    * 
FROM transform_weekdays
