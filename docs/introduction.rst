Introduction
=============

`Dataduct <https://github.com/coursera/dataduct>`__ is a wrapper built
on top of `AWS
Datapipeline <http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/what-is-datapipeline.html>`__
which makes it easy to create ETL jobs. All jobs can be specified as a
series of steps in a YAML file and would automatically be translated
into datapipeline with appropriate pipeline objects.

Features include:

- Visualizing pipeline activities
- Extracting data from different sources such as RDS, S3, local files
- Transforming data using EC2 and EMR
- Loading data into redshift
- Transforming data inside redshift
- QA data between the source system and warehouse
It is easy to create custom steps to augment the DSL as per the
requirements. As well as running a backfill with the command line
interface.

An example ETL from RDS would look like:

.. code:: YAML

    name: example_upsert
    frequency: daily
    load_time: 01:00  # Hour:Min in UTC

    steps:
    -   step_type: extract-rds
        host_name: test_host
        database: test_database
        sql: |
            SELECT *
            FROM test_table;

    -   step_type: create-load-redshift
        table_definition: tables/dev.test_table.sql

    -   step_type: upsert
        source: tables/dev.test_table.sql
        destination: tables/dev.test_table_2.sql

This would first perform an extraction from the RDS database with the
``extract-rds`` step using the ``COPY ACTIVITY``. Then load the data
into the ``dev.test_table`` in redshift with the
``create-load-redshift``. Then perform an ``upsert`` with the data into
the ``test_table_2``.
