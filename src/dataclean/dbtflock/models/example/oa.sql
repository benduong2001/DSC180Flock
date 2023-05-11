
{{ config(materialized='table') }}

-- add leadtime column with integrity checks
WITH oa AS (
SELECT
ORD0.*,
OFF_A0.LOG_RATE_USD, OFF_A0.SD_LOG_RATE_USD, OFF_A0.ORDER_OFFER_AMOUNT,
ORD_LT0.LEAD_TIME
FROM {{ ref('oa_orders_temp') }} ORD0
LEFT OUTER JOIN {{ ref('offers_aggregated') }} OFF_A0 
ON ORD0.REFERENCE_NUMBERS1 = OFF_A0.REFERENCE_NUMBERS1
LEFT OUTER JOIN {{ ref('orders_lead_time_cleaned') }} ORD_LT0 
ON ORD0.REFERENCE_NUMBERS1 = ORD_LT0.REFERENCE_NUMBERS1
)
SELECT OA0.* FROM oa OA0
