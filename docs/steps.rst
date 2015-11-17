Steps and Pipeline Objects
==========================

Pipeline objects are classes that directly translate one-one from the
dataduct classes to `DP
objects <http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-pipeline-objects.html>`__.
A step is an abstraction layer that can translate into one or more
pipeline objects based on the action type. For example a ``sql-command``
step translates into a ``sql-activity`` or a ``transform`` step
translates into ``shell command activity`` and creates an output
``s3 node``.

Definition of a Step
--------------------

A step is defined as a series of properties in yaml. For example,

::

    -   step_type: extract-s3
        name: get_file
        file_uri: s3://elasticmapreduce/samples/wordcount/wordSplitter.py

defines an ``extract-s3`` step with properties ``name`` and
``file_uri``.

Common
------

These are the properties that all steps possess.

-  ``step_type``: The step type. Must be either a pre-defined step or a
   custom step. (Required)
-  ``name``: The user-defined name of the step. Will show up as part of
   the component name in DataPipeline.
-  ``input_node``: See input and output nodes.
-  ``depends_on``: This step will not run until the step(s) specified
   have finished.

Extract S3
----------

Extracts the contents from the specified file or directory in S3. May
used as input to other steps.

Properties
^^^^^^^^^^

One of: (Required)

-  ``file_uri``: The location of a single file in S3.
-  ``directory_uri``: The location of a directory in S3.

Example
^^^^^^^

::

    -   step_type: extract-s3
        file_uri: s3://elasticmapreduce/samples/wordcount/wordSplitter.py

Extract Local
-------------

Extracts the contents from the specified file locally. May be used as
input to other steps. May only be used with one-time pipelines.

Properties
^^^^^^^^^^

-  ``path``: The location of a single file. (Required)

Example
^^^^^^^

::

    -   step_type: extract-local
        path: data/example_file.tsv

Extract RDS
-----------

Extracts the contents of a table from an RDS instance. May be used as
input to other steps. Data is stored in TSV format.

Properties
^^^^^^^^^^

-  ``host_name``: The host name to lookup in the ``mysql`` section of
   the configuration file. (Required)
-  ``database``: The database in the RDS instance in which the table
   resides. (Required)
-  ``output_path``: Output the extracted data to the specified S3 path.

One of: (Required)

-  ``sql``: The SQL query to execute to extract data.
-  ``table``: The table to extract. Equivalent to a sql query of
   ``SELECT * FROM table``.

Example
^^^^^^^

::

    -   step_type: extract-rds
        host_name: maestro
        database: maestro
        sql: |
            SELECT *
            FROM example_rds_table;

Extract Redshift
-------------------------

Extracts the contents of a table from a Redshift instance. May be used
as input to other steps. Data is stored in TSV format.

Properties
^^^^^^^^^^

-  ``schema``: The schema of the table. (Required)
-  ``table``: The name of the table. (Required)
-  ``output_path``: Output the extracted data to the specified S3 path.
   Optional.

Example
^^^^^^^

::

    -   step_type: extract-redshift
        schema: prod
        table: example_redshift_table

Transform
-------------------------

Runs a specified script on an resource.

Properties
^^^^^^^^^^

-  ``output_node``: See input and output nodes.
-  ``script_arguments``: Arguments passed to the script.
-  ``script_name``: Required if ``script_directory`` is specified.
   Script to be executed in the directory.
-  ``additional_s3_files``: Additional files to include from S3.
-  ``output_path``: Save the script's output to the specified S3 path.
-  ``no_output``: If ``true``, step will produce no extractable output.
   Default: ``false``
-  ``resource_type``: If ``ec2``, run step on the EC2 resource. If ``emr``, run
   step on the EMR resource. Default: ``ec2``

One of: (Required)

-  ``command``: A command to be executed directly.
-  ``script``: Local path to the script that should be executed.
-  ``script_directory``: Local path to a directory of scripts to be
   uploaded to the resource.

Example
^^^^^^^

::

    -   step_type: transform
        script: scripts/example_script.py
        script_arguments:
        -   "--foo=bar"

SQL Command
-------------------------

Executes a SQL statement in a Redshift instance.

Properties
^^^^^^^^^^

-  ``script_arguments``: Arguments passed to the SQL command.
-  ``queue``: Query queue that should be used.
-  ``wrap_transaction``: If ``true``, SQL command will be wrapped inside
   a transaction. Default: ``true``

One of: (Required)

-  ``command``: Command to be executed directly.
-  ``script``: Local path to the script that should be executed.

Example
^^^^^^^

::

    -   step_type: sql-command
        command: SELECT * FROM dev.test_table;

EMR Streaming
-------------------------

Executes a map and an optional reduce script using Amazon Elastic
MapReduce.

Properties
^^^^^^^^^^

-  ``mapper``: Local path to the mapper script (Required)
-  ``reducer``: Local path to the reducer script
-  ``hadoop_params``: List of arguments to the hadoop command
-  ``output_path``: Save the script's output to the specified S3 path

Example
^^^^^^^

::

    -   step_type: emr-streaming
        mapper: scripts/word_mapper.py
        reducer: scripts/word_reducer.py

Load Redshift
-------------------------

Loads the data from its input node into a Redshift instance.

Properties
^^^^^^^^^^

-  ``schema``: The schema of the table. (Required)
-  ``table``: The name of the table. (Required)
-  ``insert_mode``: See Amazon's RedshiftCopyActivity documentation.
   Default: TRUNCATE
-  ``max_errors``: The maximum number of errors to be ignored during the
   load
-  ``replace_invalid_char``: Character to replace non-utf8 characters
   with

Example
^^^^^^^

::

    -   step_type: load-redshift
        schema: dev
        table: example_table

Pipeline Dependencies
-------------------------

Keeps running until another pipeline has finished. Use with
``depends_on`` properties to stall the pipeline.

Properties
^^^^^^^^^^

-  ``dependent_pipelines``: List of pipelines to wait for. (Required)
-  ``refresh_rate``: Time, in seconds, to wait between polls. Default:
   300
-  ``start_date``: Date on which the pipelines started at. Default:
   Current day

Example
^^^^^^^

::

    -   step_type: pipeline-dependencies
        refresh_rate: 60
        dependent_pipelines:
        -   example_transform

Create Load Redshift
-------------------------

Special transform step that loads the data from its input node into a
Redshift instance. If the table it's loading into does not exist, the
table will be created.

Properties
^^^^^^^^^^

-  ``table_definition``: Schema file for the table to be loaded.
   (Required)
-  ``script_arguments``: Arguments for the runner.

   -  ``--max_error``: The maximum number of errors to be ignored during
      the load. Usage: ``--max_error=5``
   -  ``--replace_invalid_char``: Character the replace non-utf8
      characters with. Usage: ``--replace_invalid_char='?'``
   -  ``--no_escape``: If passed, does not escape special characters.
      Usage: ``--no_escape``
   -  ``--gzip``: If passed, compresses the output with gzip. Usage:
      ``--gzip``
   -  ``--command_options``: A custom SQL string as the options for the
      copy command. Usage: ``--command_options="DELIMITER '\t'"``

      -  Note: If ``--command_options`` is passed, script arguments
         ``--max_error``, ``--replace_invalid_char``, ``--no_escape``,
         and ``--gzip`` have no effect.

Example
^^^^^^^

::

    -   step_type: create-load-redshift
        table_definition: tables/dev.example_table.sql

Load, Reload, Primary Key Check
----------------------------------

Combine ``create-load-redshift``, ``reload`` and ``primary-key-check`` into one single step.

Properties
^^^^^^^^^^

-  ``staging_table_definition``: Intermidiate staging schema file for the table to be loaded into.
   (Required)
-  ``production_table_definition``: Production schema file for the table to be reloaded into.
   (Required)
-  ``script_arguments``: Arguments for the runner.

   -  ``--max_error``: The maximum number of errors to be ignored during
      the load. Usage: ``--max_error=5``
   -  ``--replace_invalid_char``: Character the replace non-utf8
      characters with. Usage: ``--replace_invalid_char='?'``
   -  ``--no_escape``: If passed, does not escape special characters.
      Usage: ``--no_escape``
   -  ``--gzip``: If passed, compresses the output with gzip. Usage:
      ``--gzip``
   -  ``--command_options``: A custom SQL string as the options for the
      copy command. Usage: ``--command_options="DELIMITER '\t'"``

      -  Note: If ``--command_options`` is passed, script arguments
         ``--max_error``, ``--replace_invalid_char``, ``--no_escape``,
         and ``--gzip`` have no effect.

Example
^^^^^^^

::

    -   step_type: load-reload-pk
        staging_table_definition: tables/staging.example_table.sql
        production_table_definition: tables/dev.example_table.sql
        script_arguments:
        -   "--foo=bar"

Upsert
-------------------------

Extracts data from a Redshift instance and upserts the data into a
table. Upsert = Update + Insert. If a row already exists (by matching
primary keys), the row will be updated. If the row does not already
exist, insert the row. If the table it's upserting into does not exist,
the table will be created.

Properties
^^^^^^^^^^

-  ``destination``: Schema file for the table to upsert into. (Required)
-  ``enforce_primary_key``: If true, de-duplicates data by matching
   primary keys. Default: true
-  ``history``: Schema file for the history table to record the changes
   in the destination table.
-  ``analyze_table``: If true, runs ``ANALYZE`` on the table afterwards.
   Default: true

One of: (Required)

-  ``sql``: The SQL query to run to extract data.
-  ``script``: Local path to a SQL query to run.
-  ``source``: The table to extract. Equivalent to a sql query of
   ``SELECT * FROM source``.

Example
^^^^^^^

::

    -   step_type: upsert
        source: tables/dev.example_table.sql
        destination: tables/dev.example_table_2.sql

Reload
-------------------------

Extracts data from a Redshift instance and reloads a table with the
data. If the table it's reloading does not exist, the table will be
created.

Properties
^^^^^^^^^^

-  ``destination``: Schema file for the table to reload. (Required)
-  ``enforce_primary_key``: If true, de-duplicates data by matching
   primary keys. Default: true
-  ``history``: Schema file for the history table to record the changes
   in the destination table.
-  ``analyze_table``: If true, runs ``ANALYZE`` on the table afterwards.
   Default: true

One of: (Required)

-  ``sql``: The SQL query to run to extract data.
-  ``script``: Local path to a SQL query to run.
-  ``source``: The table to extract. Equivalent to a sql query of
   ``SELECT * FROM source``.

Example
^^^^^^^

::

    -   step_type: reload
        source: tables/dev.example_table.sql
        destination: tables/dev.example_table_2.sql

Create Update SQL
-------------------------

Creates a table if it exists and then runs a SQL command.

Properties
^^^^^^^^^^

-  ``table_definition``: Schema file for the table to create. (Required)
-  ``script_arguments``: Arguments for the SQL script.
-  ``non_transactional``: If true, does not wrap the command in a
   transaction. Default: false
-  ``analyze_table``: If true, runs ``ANALYZE`` on the table afterwards.
   Default: true

One of: (Required)

-  ``command``: SQL command to execute directly.
-  ``script``: Local path to a SQL command to run.

Example
^^^^^^^

::

    -   step_type: create-update-sql
        command: |
            DELETE FROM dev.test_table WHERE id < 0;
            INSERT INTO dev.test_table
            SELECT * FROM dev.test_table_2
            WHERE id < %s;
        table_definition: tables/dev.test_table.sql
        script_arguments:
        -   4

Primary Key Check
-------------------------

Checks for primary key violations on a specific table.

Properties
^^^^^^^^^^

-  ``table_definition``: Schema file for the table to check. (Required)
-  ``script_arguments``: Arguments for the runner script.
-  ``log_to_s3``: If true, logs the output to a file in S3. Default:
   false

Example
^^^^^^^

::

    -   step_type: primary-key-check
        table_definition: tables/dev.test_table.sql

Count Check
-------------------------

Compares the number of rows in the source and destination tables/SQL
scripts.

Properties
^^^^^^^^^^

-  ``source_host``: The source host name to lookup in the ``mysql``
   section of the configuration file. (Required)
-  ``tolerance``: Tolerance threshold, in %, for the difference in count
   between source and destination. Default: 1
-  ``log_to_s3``: If true, logs the output to a file in S3. Default:
   false
-  ``script``: Replace the default count script.
-  ``script_arguments``: Arguments for the script.

One of: (Required)

-  ``source_sql``: SQL query to select rows to count for the source.
-  ``source_count_sql``: SQL query that returns a count for the source.
-  ``source_table_name``: Name of source table to count. Equivalent to a
   source\_count\_sql of ``SELECT COUNT(1) from source_table_name``.

One of: (Required)

-  ``destination_sql``: SQL query to select rows to count for the
   destination.
-  ``destination_table_name``: Name of the destination table to count.
-  ``destination_table_definition``: Schema file for the destination
   table to count.

Example
^^^^^^^

::

    -   step_type: count-check
        source_sql: "SELECT id, name FROM networks_network;"
        source_host: maestro
        destination_sql: "SELECT network_id, network_name FROM prod.networks"
        tolerance: 2.0
        log_to_s3: true

Column Check
-------------------------

Compares a sample of rows from the source and destination tables/SQL
scripts to see if they match

Properties
^^^^^^^^^^

-  ``source_host``: The source host name to lookup in the ``mysql``
   section of the configuration file. (Required)
-  ``source_sql``: SQL query to select rows to check for the source.
   (Required)
-  ``sql_tail_for_source``: Statement to append at the end of the SQL
   query for the source
-  ``sample_size``: Number of samples to check. Default: 100
-  ``tolerance``: Tolerance threshold, in %, for mismatched rows.
   Default: 1
-  ``log_to_s3``: If true, logs the output to a file in S3. Default:
   false
-  ``script``: Replace the default column check script.
-  ``script_arguments``: Arguments for the script.

One of: (Required)

-  ``destination_sql``: SQL query to select rows to check for the
   destination.
-  ``destination_table_definition``: Schema file for the destination
   table to check.

Example
^^^^^^^

::

    -   step_type: column-check
        source_sql: "SELECT id, name FROM networks_network;"
        source_host: maestro
        destination_sql: "SELECT network_id, network_name FROM prod.networks"
        sql_tail_for_source: "ORDER BY RAND() LIMIT LIMIT_PLACEHOLDER"
        sample_size: 10
        log_to_s3: true
