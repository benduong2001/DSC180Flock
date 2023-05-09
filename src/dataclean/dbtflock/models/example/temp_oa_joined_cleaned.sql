
{{ config(materialized='table') }}

WITH
temp_oa_joined AS
(
SELECT
ORD0.REFERENCE_NUMBERS1,
{% if filtering_ftl != 1 %}
ORD0.TRANSPORT_MODE,
{% endif %}
ORD0.ORIGIN_3DIGIT_ZIP,
ORD0.DESTINATION_3DIGIT_ZIP,
ORD0.X_COORD_ORIG, ORD0.X_COORD_DEST, ORD0.Y_COORD_ORIG, ORD0.Y_COORD_DEST,
ORD0.TEMP_TIME_UNIT_SECOND, 
ORD0.ORDER_DATETIME_PST,
ORD0.TIME_BETWEEN_ORDER_AND_DEADLINE,
OFF0.CREATED_ON_HQ,
OFF0.LOAD_DELIVERED_FROM_OFFER,
OFF0.RATE_USD,
OFF0.OFFER_TYPE
FROM 
{{ ref('oa_orders_temp') }} ORD0
LEFT OUTER JOIN
{{ ref('oa_offers_temp') }} OFF0
ON
ORD0.REFERENCE_NUMBERS1 = OFF0.REFERENCE_NUMBERS1
),

-- cleans the joined table. Specifically, all rows where Transport mode = FTL should only correspond with 
-- offers with 1 order and offers of the type quote
-- if FTL's were filtered beforehand, that means just filter for quote = 1.
temp_oa_joined_cleaned AS
(
SELECT
OA0.*
FROM
temp_oa_joined OA0
{% if filtering_ftl == 1 %}
EXCEPT (SELECT OA1.* FROM temp_oa_joined OA1 WHERE OA1.TRANSPORT_MODE = 'FTL' AND OA1.OFFER_TYPE != 'quote')
{% else %}
WHERE OA0.OFFER_TYPE != 'quote' -- assumes transport_mode column isn't present and all of order's rows are FTL anyways
{% endif %}
)

SELECT * FROM temp_oa_joined_cleaned;

