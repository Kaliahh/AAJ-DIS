DROP TABLE IF EXISTS product CASCADE;

CREATE TABLE product (
	productid serial PRIMARY KEY,
	product_name varchar(100),
	product_type varchar(100),
	category varchar(100),
	subcategory varchar(100),
	status varchar(10),
	alcohol_content_ml double precision
);

INSERT INTO product (product_name, status, alcohol_content_ml) values ('undefined', 'inactive', 0);

DROP TABLE IF EXISTS member CASCADE;

CREATE TABLE member (
	memberid SERIAL PRIMARY KEY,
	gender varchar(20),
	is_active boolean
);

--INSERT INTO member (year_created, gender, sourceid) values (0000, 'Undefined', 0);

DROP TABLE IF EXISTS room CASCADE;

CREATE TABLE room (
	roomid SERIAL PRIMARY KEY,
	room_name varchar(100)
);

INSERT INTO room (room_name) values ('Unknown room');

DROP TABLE IF EXISTS time CASCADE;

CREATE TABLE time(
	timeid SERIAL PRIMARY KEY,
	year int,
	month int,
	day int,
	time_of_day varchar(10),
	season varchar(10),
	day_of_week varchar(10),
	is_weekday varchar(10),
	holiday varchar(50),
	event varchar(50)
);
	
DROP TABLE IF EXISTS salesfact;

CREATE TABLE salesfact(
	timeid int,
	productid int,
	memberid int, 
	roomid int,
	kroner_sales numeric(10, 2),
	unit_sales int,
	PRIMARY KEY (timeid, productid, memberid, roomid),
	FOREIGN KEY (timeid) REFERENCES time (timeid),
	FOREIGN KEY (productid) REFERENCES product (productid),
	FOREIGN KEY (memberid) REFERENCES member (memberid),
	FOREIGN KEY (roomid) REFERENCES room (roomid)
);