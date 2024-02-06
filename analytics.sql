-- 1. Store origin
SELECT country_code, COUNT(*) as num_stores
FROM dim_store_details
GROUP BY country_code;

-- 2. Locality ranking
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY COUNT(*) DESC
LIMIT 7;

-- 3. Total sales by month
SELECT
    ROUND(SUM(orders.product_quantity * products.product_price)::numeric, 2) AS total_sales,
    dates.month AS calendar_month
FROM
    orders_table AS orders
INNER JOIN
    dim_date_times AS dates ON orders.date_uuid = dates.date_uuid
INNER JOIN
    dim_products AS products ON orders.product_code = products.product_code
GROUP BY
    calendar_month
ORDER BY
    total_sales DESC
LIMIT 6;

-- 4. Web vs. Offline
SELECT
	COUNT(*) AS numbers_of_sales,
	SUM(product_quantity) AS product_quantity_count,
	CASE 
		WHEN s.store_type = 'Web Portal' THEN 'Web'
		ELSE 'Offline'
	END AS location
FROM
	orders_table o
INNER JOIN
	dim_store_details s ON o.store_code = s.store_code
GROUP BY
	location;

-- 5. Sales of Store Types
WITH total_sales_cte AS (
    SELECT
        store_type,
        SUM(pr.product_price * o.product_quantity)::numeric AS total_sales
    FROM
        orders_table o
    INNER JOIN
        dim_store_details s ON o.store_code = s.store_code
    INNER JOIN
        dim_products pr ON o.product_code = pr.product_code
    GROUP BY
        store_type
)
SELECT
    store_type,
    ROUND(total_sales, 2) AS total_sales,
    ROUND((total_sales / (SELECT SUM(total_sales) FROM total_sales_cte)) * 100, 2) AS percentage_total
FROM
    total_sales_cte
ORDER BY 
    total_sales DESC;

-- 6. Best sales historically by year and month
SELECT
	SUM(pr.product_price * o.product_quantity)::numeric AS total_sales,
	dt.year AS sale_year,
	dt.month AS sale_month
FROM 
	orders_table o
INNER JOIN
	dim_products pr ON o.product_code = pr.product_code
INNER JOIN
	dim_date_times dt ON o.date_uuid = dt.date_uuid
GROUP BY
    sale_year, sale_month
ORDER BY
	total_sales DESC
LIMIT 10;

-- 7. Staff headcount
SELECT
	SUM(staff_numbers) AS staff,
	country_code
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY 
	staff DESC;

-- 8. Best-selling German stores
SELECT
	ROUND(SUM(pr.product_price * o.product_quantity)::numeric, 2) AS total_sales,
	sd.store_type AS store_type,
	sd.country_code AS country_code
FROM 
	orders_table o
INNER JOIN
	dim_products pr ON o.product_code = pr.product_code
INNER JOIN
	dim_store_details sd ON o.store_code = sd.store_code
WHERE
    sd.country_code = 'DE'
GROUP BY
	store_type, country_code
ORDER BY
	total_sales ASC;

-- 9. Yearly averages of sale speed
WITH sales_data AS (
    SELECT
        *,
        LEAD(CONCAT(year, '-', month, '-', day, ' ', timestamp)::timestamp) 
		OVER (PARTITION BY year ORDER BY CONCAT(year, '-', month, '-', day, ' ', timestamp)::timestamp) 
		AS next_sale_timestamp
    FROM
        dim_date_times
)
SELECT
    year,
    json_build_object(
        'hours', EXTRACT(HOUR FROM AVG(next_sale_timestamp - CONCAT(year, '-', month, '-', day, ' ', timestamp)::timestamp)),
        'minutes', EXTRACT(MINUTE FROM AVG(next_sale_timestamp - CONCAT(year, '-', month, '-', day, ' ', timestamp)::timestamp)),
        'seconds', EXTRACT(SECOND FROM AVG(next_sale_timestamp - CONCAT(year, '-', month, '-', day, ' ', timestamp)::timestamp)),
        'milliseconds', EXTRACT(MILLISECOND FROM AVG(next_sale_timestamp - CONCAT(year, '-', month, '-', day, ' ', timestamp)::timestamp))
    ) AS actual_time_taken
FROM
    sales_data
GROUP BY
    year
ORDER BY
    EXTRACT(EPOCH FROM AVG(next_sale_timestamp - CONCAT(year, '-', month, '-', day, ' ', timestamp)::timestamp)) * 1000 DESC
LIMIT 5;