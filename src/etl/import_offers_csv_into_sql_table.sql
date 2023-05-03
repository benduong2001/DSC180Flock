
DROP TABLE IF EXISTS offer_acceptance_offers;
CREATE TABLE offer_acceptance_offers (
    CARRIER_ID varchar(64),
    REFERENCE_NUMBER varchar(1000),
    CREATED_ON_HQ timestamp ,
    RATE_USD double,
    OFFER_TYPE varchar(10),
    SELF_SERVE boolean,
    IS_OFFER_APPROVED boolean,
    AUTOMATICALLY_APPROVED boolean,
    MANUALLY_APPROVED boolean ,
    WAS_EVER_UNCOVERED boolean,
    COVERING_OFFER boolean,
    LOAD_DELIVERED_FROM_OFFER boolean,
    RECOMMENDED_LOAD boolean,
    VALID boolean         
);
COPY offer_acceptance_offers FROM '{path_file_csv}' WITH CSV HEADER;
