CREATE TABLE orders (
    order_id INTEGER DISTKEY PRIMARY KEY
    ,customer_id INTEGER REFERENCES customers(customer_id)
    ,employee_id INTEGER REFERENCES employees(employee_id)
    ,order_date DATE
    ,shipper_id INTEGER REFERENCES shippers(shipper_id)
) SORTKEY(order_id);
