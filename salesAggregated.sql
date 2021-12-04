SELECT 
	f.year,
    f.month,
    f.season,
    f.day,
    f.day_of_week,
    f.time_of_day,
    f.p_name AS product_name,
    f.r_name AS room_name,
    f.gender,
    f.active as is_active,
    ROUND((SUM(f.price)::float / 100)::numeric, 2) AS kroner_sales,
    COUNT(*) AS unit_sales
FROM (SELECT s.id, m.gender, m.active, p.name AS p_name, r.name AS r_name, s.price,
    DATE_PART('year',timestamp) AS year, DATE_PART('month',timestamp) AS month, DATE_PART('day',timestamp) AS day, DATE_PART('hour',timestamp) AS hour,
    CASE
    	WHEN DATE_PART('hour',timestamp) IN (6, 7, 8, 9, 10) THEN 'Morning'
        WHEN DATE_PART('hour',timestamp) IN (11, 12, 13) THEN 'Noon'
        WHEN DATE_PART('hour',timestamp) IN (13, 14, 15, 16) THEN 'Afternoon'
        ELSE 'Night'
	END AS time_of_day,
    to_char(s.timestamp, 'Day') AS day_of_week,
    CASE
    	WHEN DATE_PART('month',timestamp) IN (12, 1, 2) THEN 'Winter'
        WHEN DATE_PART('month',timestamp) IN (3, 4, 5) THEN 'Spring'
        WHEN DATE_PART('month',timestamp) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END as season
    FROM stregsystem.stregsystem_sale s
	INNER JOIN stregsystem.stregsystem_product AS p ON p.id = s.product_id
    INNER JOIN stregsystem.stregsystem_room AS r ON r.id = s.room_id
    INNER JOIN stregsystem.stregsystem_member AS m ON m.id = s.member_id) f 
GROUP BY f.active, f.gender, f.p_name, f.r_name, f.year, f.season, f.month, f.day, f.day_of_week, f.time_of_day