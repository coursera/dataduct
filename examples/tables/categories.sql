CREATE TABLE categories (
    category_id INTEGER DISTKEY PRIMARY KEY
    ,category_name VARCHAR(100)
    ,description VARCHAR(2000)
) SORTKEY(category_id);
