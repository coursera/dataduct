CREATE TABLE shippers (
    shipper_id INTEGER DISTKEY PRIMARY KEY
    ,shipper_name VARCHAR(200)
    ,phone VARCHAR(20)
) SORTKEY(shipper_id);
