CREATE TABLE employees (
    employee_id INTEGER DISTKEY PRIMARY KEY
    ,last_name VARCHAR(100)
    ,first_name VARCHAR(100)
    ,birth_date DATE
    ,notes VARCHAR(2000)
) SORTKEY(employee_id);
