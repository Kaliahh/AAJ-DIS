DROP TABLE IF EXISTS time CASCADE;

CREATE TABLE time(
	timeid SERIAL PRIMARY KEY,
	year int,
	month int,
	day int,
    hour int,
	time_of_day varchar(10),
	season varchar(10),
	day_of_week varchar(10),
	is_weekday varchar(10),
	holiday varchar(50),
	event varchar(50)
);

DROP TABLE IF EXISTS sales CASCADE;

CREATE TABLE sales(
    sale_id SERIAL PRIMARY KEY,
	timeid int,
	productid int,
	memberid int, 
	roomid int,
	kroner_sales numeric(10, 2),
	unit_sales int
);