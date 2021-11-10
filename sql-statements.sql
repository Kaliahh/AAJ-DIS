DROP TABLE IF EXISTS product;

CREATE TABLE product (
    productid serial PRIMARY KEY,
    product_name varchar(100),
    product_type varchar(100),
    category varchar(100),
    subcategory varchar(100),
    isActive boolean,
    alchohol_pct double precision
);