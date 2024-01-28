/* 
SQL queries to cast correct data types, set up primary keys and foreign key constraints.
Executes on the orders truth table, as well as the users, stores, products, date times 
and card details tables.
*/


-- Orders Table
ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT;

-- Users Table
ALTER TABLE dim_users ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN join_date TYPE DATE,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ADD PRIMARY KEY (user_uuid);
ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);

-- Stores Table
ALTER TABLE dim_store_details DROP COLUMN IF EXISTS index,
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT,	
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255),
ADD PRIMARY KEY (store_code);
ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);


-- Products Table
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '')
WHERE product_price LIKE '£%';
ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(14);
UPDATE dim_products
SET weight_class = 
    CASE 
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        WHEN weight >= 140 THEN 'Truck_Required'
        ELSE NULL
    END;
ALTER TABLE dim_products RENAME COLUMN removed to still_available;
ALTER TABLE dim_products ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
ALTER COLUMN weight TYPE FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN "uuid" TYPE UUID USING "uuid"::UUID,
ALTER COLUMN still_available TYPE BOOL,
ALTER COLUMN weight_class TYPE VARCHAR(14);
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);
ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code) REFERENCES dim_products (product_code);

-- Date Times Table
ALTER TABLE dim_date_times ALTER COLUMN "month" TYPE VARCHAR(2),
ALTER COLUMN "year" TYPE VARCHAR(4),
ALTER COLUMN "day" TYPE VARCHAR(2),
ALTER COLUMN "time_period" TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);
ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);

-- Card Details Table
ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE;
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);
ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);
