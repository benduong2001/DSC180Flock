
DROP TABLE IF EXISTS offer_acceptance_orders;
CREATE TABLE IF NOT EXISTS offer_acceptance_orders AS
SELECT * FROM read_csv_auto('C:\Users\Benson\Desktop\college\college_year_4\2022_fall\dsc180a\archive_temp\dbt_sample\data\raw\zipcode_coordinates.csv');

DROP TABLE IF EXISTS offer_acceptance_offers;
CREATE TABLE IF NOT EXISTS offer_acceptance_offers AS
SELECT * FROM read_csv_auto('C:\Users\Benson\Desktop\college\college_year_4\2022_fall\dsc180a\archive_temp\dbt_sample\data\raw\offer_acceptance_offers.csv');

DROP TABLE IF EXISTS zipcode_coordinates;
CREATE TABLE IF NOT EXISTS zipcode_coordinates AS
SELECT * FROM read_csv_auto('C:\Users\Benson\Desktop\college\college_year_4\2022_fall\dsc180a\archive_temp\dbt_sample\data\raw\offer_acceptance_orders.csv');
