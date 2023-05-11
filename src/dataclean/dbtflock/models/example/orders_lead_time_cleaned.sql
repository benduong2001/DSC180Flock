
{{ config(materialized='table') }}

-- add leadtime column with integrity checks
WITH 
orders_lead_time AS (
SELECT
OA0.REFERENCE_NUMBERS1,
(
CASE 
WHEN
(least(OA0.ORDER_DATETIME_PST,OA0.CREATED_ON_HQ) = OA0.ORDER_DATETIME_PST)
THEN
(CAST(date_diff(OA0.TEMP_TIME_UNIT_SECOND, OA0.ORDER_DATETIME_PST,OA0.CREATED_ON_HQ) AS DECIMAL)/(OA0.TIME_BETWEEN_ORDER_AND_DEADLINE + 0.0001))
ELSE
NULL
END
)
AS LEAD_TIME
FROM
{{ ref('temp_oa_joined_cleaned') }} OA0
WHERE OA0.LOAD_DELIVERED_FROM_OFFER = true
),

orders_lead_time_cleaned AS 
(
SELECT
OLT0.* FROM orders_lead_time OLT0
WHERE LEAD_TIME > 0.0 OR LEAD_TIME IS NOT NULL
)

SELECT OLTC.* FROM orders_lead_time_cleaned OLTC