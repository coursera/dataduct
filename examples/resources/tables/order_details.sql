CREATE TABLE order_details (
    order_detail_id INTEGER DISTKEY PRIMARY KEY
    ,order_id INTEGER REFERENCES orders(order_id)
    ,product_id INTEGER REFERENCES products(product_id)
    ,quantity INTEGER
) SORTKEY(order_detail_id);
