
{{ config(materialized='table') }}

-- add a new version of the reference number column cleaned as a list of string references to the orders table
WITH 
orders_references AS
(
SELECT ORD0.*,
regexp_split_to_array(
replace(
replace(
replace(
replace(
replace(
ORD0.REFERENCE_NUMBER
, chr(10), '') 
-- newline
, '"', '')
, ' ', '')
, '[', '')
, ']', '')
, 
',') 
as REFERENCE_NUMBERS,
FROM 
offer_acceptance_orders ORD0
),



-- optional filtering of just FTLs (must also drop transport mode)
orders_transport AS 
(
SELECT ORD0.*
{% if filtering_ftl == 1 %}
EXCEPT (SELECT ORD1.TRANSPORT_MODE FROM orders_references ORD1)
{% endif %}
FROM 
orders_references ORD0
{% if filtering_ftl == 1 %}
WHERE ORD0.TRANSPORT_MODE = 'FTL'
{% endif %}
),



-- add the x/y coordinate columns of the orders' origin and destination zipcodes to orders table
-- also shrinks the x/y coordinate columns by a factor of 1 million
orders_zipcoord AS
(
SELECT ORD0.*, 
ZIP_ORIG.X_COORD/1000000 as X_COORD_ORIG,
ZIP_ORIG.Y_COORD/1000000 as Y_COORD_ORIG,
ZIP_DEST.X_COORD/1000000 as X_COORD_DEST,
ZIP_DEST.Y_COORD/1000000 as Y_COORD_DEST
FROM 
orders_transport ORD0
LEFT OUTER JOIN
zipcode_coordinates ZIP_ORIG
ON ORD0.ORIGIN_3DIGIT_ZIP = ZIP_ORIG."3DIGIT_ZIP"
LEFT OUTER JOIN 
zipcode_coordinates ZIP_DEST
ON ORD0.DESTINATION_3DIGIT_ZIP = ZIP_DEST."3DIGIT_ZIP"
),



-- crops the zipcode nodes of orders table to be only within the mainland US    
orders_zipcoords_mainland_us AS
(
SELECT ORD0.*
FROM 
orders_zipcoord ORD0
WHERE
ORD0.X_COORD_ORIG >= -15 AND ORD0.X_COORD_ORIG <=  -7 AND
ORD0.Y_COORD_ORIG >= 2.5 AND ORD0.Y_COORD_ORIG <= 6.5 AND
ORD0.X_COORD_DEST >= -15 AND ORD0.X_COORD_DEST <=  -7 AND
ORD0.Y_COORD_DEST >= 2.5 AND ORD0.Y_COORD_DEST <= 6.5
),



-- add euclidean distance column of shipping routes to orders table
orders_distance AS
(
SELECT 
ORD0.*,
(POWER((ORD0.X_COORD_DEST-ORD0.X_COORD_ORIG),2) + POWER((ORD0.Y_COORD_DEST-ORD0.Y_COORD_ORIG),2)) AS DISTANCE
FROM orders_zipcoords_mainland_us ORD0
),



-- add columns of the month, weekday, and hour of an order's date of posting and deadline to the orders table
orders_time_categ AS
(
SELECT ORD0.*,
extract("month" FROM ORD0.ORDER_DATETIME_PST) AS ORDER_MONTH,
dayname(ORD0.ORDER_DATETIME_PST) AS ORDER_DAY,
extract("hour" FROM ORD0.ORDER_DATETIME_PST) AS ORDER_HOUR,
extract("month" FROM ORD0.PICKUP_DEADLINE_PST) AS DEADLINE_MONTH,
dayname(ORD0.PICKUP_DEADLINE_PST) AS DEADLINE_DAY,
extract("hour" FROM ORD0.PICKUP_DEADLINE_PST) AS DEADLINE_HOUR,
'second' as TEMP_TIME_UNIT_SECOND,
FROM orders_distance ORD0
),



-- add column on time between order's posting and its deadline to orders table
-- also performs data integrity check that returns null if the duration is "impossible" (i.e. <= 0)
orders_duration AS 
(
SELECT ORD1.*,
(
CASE 
WHEN 
(
((least(ORD1.ORDER_DATETIME_PST, ORD1.PICKUP_DEADLINE_PST) = ORD1.ORDER_DATETIME_PST) 
AND
(ORD1.ORDER_DATETIME_PST != ORD1.PICKUP_DEADLINE_PST)) 
)
THEN
date_diff(ORD1.TEMP_TIME_UNIT_SECOND, ORD1.ORDER_DATETIME_PST, ORD1.PICKUP_DEADLINE_PST)
ELSE
NULL
END
)
AS TIME_BETWEEN_ORDER_AND_DEADLINE
FROM 
orders_time_categ ORD1
),



-- adds a column of distance over duration (with a small positive number .0001 added to prevent zero division) to orders table
orders_speed AS
(
SELECT 
ORD0.*,
((ORD0.DISTANCE) / (ORD0.TIME_BETWEEN_ORDER_AND_DEADLINE) + 0.0001) as DISTANCE_OVER_ORDER_TIME
FROM
orders_duration ORD0
),



-- adds the delivery route geodata columns to orders table
orders_delivery_data AS
(
SELECT *
FROM
orders_speed ORD0
-- LEFT OUTER JOIN (SELECT * FROM delivery_route_geodata DRGD0) ON DRGD0.REFERENCE_NUMBER1 = ORD0.REFERENCE_NUMBER1
),



-- this unnests the list-converted reference numbers column, expanding the row count and changing the row schema
orders_unnested AS
(
SELECT 
ORD0.*, 
-- dbt jinja if-statement: if dont_explode_references was set to 0, unnest the reference numbers, else reuse the pre-list-converted
{% if dont_explode_references == 0 %}
ORD0.REFERENCE_NUMBER AS REFERENCE_NUMBERS1
{% else %}
UNNEST(ORD0.REFERENCE_NUMBERS) AS REFERENCE_NUMBERS1 
{% endif %}
FROM orders_delivery_data ORD0
)



SELECT ORD0.* 
FROM orders_unnested ORD0

