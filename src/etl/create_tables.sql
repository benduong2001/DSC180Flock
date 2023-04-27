
DROP TABLE IF EXISTS offer_acceptance_orders;
CREATE TABLE IF NOT EXISTS offer_acceptance_orders AS
SELECT * FROM read_csv_auto('{path_file_oa_orders}');

DROP TABLE IF EXISTS offer_acceptance_offers;
CREATE TABLE IF NOT EXISTS offer_acceptance_offers AS
SELECT * FROM read_csv_auto('{path_file_oa_offers}');

DROP TABLE IF EXISTS zipcode_coordinates;
CREATE TABLE IF NOT EXISTS zipcode_coordinates AS
SELECT * FROM read_csv_auto('{path_file_zipcode_coordinates}');
