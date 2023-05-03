{{ config(materialized='table') }}

-- applies a data integrity check that removes rows that aren't valid. 
-- Specifically, rows where offer type is quote should have a reference number column with only one reference 
-- and rows where offer type is pool should have many references.
-- This is a row-decreasing operation
WITH 
offer_acceptance_offers_correct AS (
SELECT OFF0.*
FROM offer_acceptance_offers OFF0
WHERE 
(OFF0.OFFER_TYPE = 'quote') AND (len(regexp_split_to_array(OFF0.REFERENCE_NUMBER, ',')) = 1) 
OR
(OFF0.OFFER_TYPE = 'pool') AND (len(regexp_split_to_array(OFF0.REFERENCE_NUMBER, ',')) >= 1)
),

-- add a new version of the reference number column cleaned as a list of string references to the offers table
offer_references AS (
SELECT
OFF0.*,
regexp_split_to_array(
replace(
replace(
replace(
replace(
replace(
OFF0.REFERENCE_NUMBER
, '\n', '')
, '"', '')
, ' ', '')
, '[', '')
, ']', '')
, 
',') 
as REFERENCE_NUMBERS,
FROM offer_acceptance_offers_correct OFF0
),

-- this unnests the list-converted reference numbers column, expanding the row count and changing the row schema
offers_unnested AS
(
SELECT 
OFF0.*,
-- dbt jinja if-statement: if explode_references was set to 1, unnest the reference numbers, else reuse the pre-list-converted
{% if explode_references == 1 %}
unnest(OFF0.REFERENCE_NUMBERS) AS REFERENCE_NUMBERS1
{% else %}
OFF0.REFERENCE_NUMBER AS REFERENCE_NUMBERS1
{% endif %}
FROM offer_references OFF0
)

SELECT
OFF0.*
FROM offers_unnested OFF0