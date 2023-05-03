
DROP TABLE IF EXISTS zipcode_coordinates;
CREATE TABLE zipcode_coordinates (
    "3DIGIT_ZIP" varchar(10),
    X_COORD double ,
    Y_COORD double      
);
COPY zipcode_coordinates FROM '{path_file_csv}' WITH CSV HEADER;
