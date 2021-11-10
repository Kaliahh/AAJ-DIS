DROP TABLE IF EXISTS product;

CREATE TABLE product (
	productid serial PRIMARY KEY,
	product_name varchar(100),
	product_type varchar(100),
	category varchar(100),
	subcategory varchar(100),
	is_active boolean,
	alcohol_content_ml double precision
);

DROP TABLE IF EXISTS member;

CREATE TABLE member (
	memberid SERIAL PRIMARY KEY,
	year_created int,
	gender varchar(20),
	sourceid int
);

DROP TABLE IF EXISTS room;

CREATE TABLE room (
	roomid SERIAL PRIMARY KEY,
	name varchar(100)
);

DROP TABLE IF EXISTS time;

CREATE TABLE time(
	timeid SERIAL PRIMARY KEY,
	year int,
	month int,
	day int,
	time_of_day varchar(10),
	season varchar(10),
	day_of_week varchar(10),
	is_weekday boolean,
	holiday varchar(50),
	event varchar(50)
);
	
