
DROP TABLE IF EXISTS offer_acceptance_orders;
CREATE TABLE offer_acceptance_orders (
    REFERENCE_NUMBER varchar(1000) ,
    ORDER_DATETIME_PST timestamp,
    PICKUP_DEADLINE_PST timestamp,
    DELIVERY_TIME_CONSTRAINT varchar(25),
    ORIGIN_3DIGIT_ZIP varchar(3),
    DESTINATION_3DIGIT_ZIP varchar(3),
    APPROXIMATE_DRIVING_ROUTE_MILEAGE double,
    PALLETIZED_LINEAR_FEET double,
    FD_ENABLED boolean,
    EXCLUSIVE_USE_REQUESTED boolean,
    HAZARDOUS boolean,
    REEFER_ALLOWED boolean,
    STRAIGHT_TRUCK_ALLOWED boolean,
    LOAD_BAR_COUNT double, 
    LOAD_TO_RIDE_REQUESTED boolean,
    ESTIMATED_COST_AT_ORDER double,
    TRANSPORT_MODE varchar(20)
);
COPY offer_acceptance_orders FROM '{path_file_csv}' WITH CSV HEADER;
