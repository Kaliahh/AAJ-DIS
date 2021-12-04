DROP TABLE IF EXISTS Sales;

CREATE TABLE Sales(
    id SERIAL,
    year INT,
    month INT,
    season TEXT,
    day INT,
    day_of_week TEXT,
    time_of_day TEXT,
    product_name TEXT,
    room_name TEXT,
    gender TEXT,
    is_active BOOLEAN,
    kroner_sales numeric(10,2),
    unit_sales INT,
    PRIMARY KEY(id)
);