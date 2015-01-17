CREATE TABLE customers (
    customer_id INTEGER DISTKEY PRIMARY KEY
    ,customer_name VARCHAR(200)
    ,contact_name VARCHAR(200)
    ,address VARCHAR(200)
    ,city VARCHAR(100)
    ,postal_code VARCHAR(10)
    ,country VARCHAR(100)
) SORTKEY(customer_id);
