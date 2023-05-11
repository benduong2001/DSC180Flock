
{{ config(materialized='table') }}

-- add leadtime column with integrity checks
WITH 
offers_aggregated AS
(
SELECT
OA0.REFERENCE_NUMBERS1,
STDDEV_POP(OA0.RATE_USD) AS SD_LOG_RATE_USD,
AVG(LN(OA0.RATE_USD+1)) AS LOG_RATE_USD,
COUNT(*) AS ORDER_OFFER_AMOUNT
FROM
{{ ref('temp_oa_joined_cleaned') }} OA0
GROUP BY OA0.REFERENCE_NUMBERS1
--HAVING LOG_RATE_USD IS NOT NULL
)

SELECT OAG0.* FROM offers_aggregated OAG0
