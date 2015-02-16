CREATE TABLE suppliers (
    supplier_id INTEGER DISTKEY PRIMARY KEY
    ,supplier_name VARCHAR(200)
    ,contact_name VARCHAR(200)
    ,address VARCHAR(200)
    ,city VARCHAR(100)
    ,postal_code VARCHAR(10)
    ,county VARCHAR(100)
    ,phone VARCHAR(20)
) SORTKEY(supplier_id);
