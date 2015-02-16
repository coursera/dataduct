CREATE TABLE products (
    product_id INTEGER DISTKEY PRIMARY KEY
    ,product_name VARCHAR(200)
    ,supplier_id INTEGER REFERENCES suppliers(supplier_id)
    ,category_id INTEGER REFERENCES categories(category_id)
    ,unit VARCHAR(200)
    ,price REAL
) SORTKEY(product_id);
