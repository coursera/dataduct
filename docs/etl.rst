Creating an ETL
===============

Dataduct makes it extremely easy to write ETL in Data Pipeline. All the
details and logic can be abstracted in the YAML files which will be
automatically translated into Data Pipeline with appropriate pipeline
objects and other configurations.

Writing a Dataduct YAML File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To learn about general YAML syntax, please see `YAML
syntax <http://en.wikipedia.org/wiki/YAML>`__. The structure of a
Dataduct YAML file can be broken down into 3 parts:

-  Header information
-  Description
-  Pipeline steps

Example:

.. code:: yaml

    # HEADER INFORMATION
    name : example_emr_streaming   
    frequency : one-time           
    load_time: 01:00  # Hour:Min in UTC
    emr_cluster_config:
        num_instances: 1           
        instance_size: m1.xlarge   

    # DESCRIPTION
    description : Example for the emr_streaming step

    # PIPELINE STEPS
    steps:
    -   type: extract-local        
        path: examples/resources/word_data.txt
      
    -   type: emr-streaming        
        mapper: examples/scripts/word_mapper.py 
        reducer: examples/scripts/word_reducer.py
      
    -   type: transform
        script: examples/scripts/s3_profiler.py 
        script_arguments:
        -   --input=INPUT1_STAGING_DIR  
        -   --output=OUTPUT1_STAGING_DIR
        -   -f

Header Information
^^^^^^^^^^^^^^^^^^

The header includes configuration information for Data Pipeline and the
Elastic MapReduce resource.

The name field sets the overall pipeline name:

.. code:: yaml

    name : example_emr_streaming   

The frequency represents how often the pipeline is run on a schedule
basis. Currently supported intervals are *hourly, daily, one-time*:

.. code:: yaml

    frequency : one-time           

The load time is what time of day (in UTC) the pipeline is scheduled to
run. It is in the format of HH:MM so 01:00 would set the pipeline to run
at 1AM UTC:

.. code:: yaml

    load_time: 01:00  # Hour:Min in UTC

If the pipeline includes an EMR-streaming step, the EMR instance can be
configured. For example, you can configure the bootstrap, number of core
instances, and instance types:

.. code:: yaml

    emr_cluster_config:
        num_instances: 1           
        instance_size: m1.xlarge 

Description
^^^^^^^^^^^

The description allows the creator of the YAML file to clearly explain
the purpose of the pipeline.

Pipeline Steps
^^^^^^^^^^^^^^

The pipeline steps are very verbose and easy to understand, as they map
directly into Data Pipeline steps. Each step must have a type associated
with it (transform step / emr-streaming step) and should be named for
clarification purposes. The following lists every step type:

emr-streaming
'''''''''''''

The *emr-streaming* step runs on a EMR instance configured from the
header. You can specify the bootstrap, mapper, and reducer files.

.. code:: yaml

    -   type: emr-streaming        
        mapper: examples/scripts/word_mapper.py 
        reducer: examples/scripts/word_reducer.py

extract-local
'''''''''''''

The *extract-local* step will extract a local file (for example, a TSV
file) and write it to the output node. From there, the data can be
loaded into redshift or apply further transformations.

.. code:: yaml

    -   name: extract_local_step
        type: extract-local        
        path: examples/resources/word_data.txt

extract-rds
'''''''''''

The *extract-rds* step extracts data from MySQL databases to S3. You can
also specify the SQL statement that you would like to execute. This
extraction will look for tables based on the host name and the database
name which needs to be pre-configured in ~/.dataduct

.. code:: yaml

    -   type: extract-rds
        host_name: maestro
        database: maestro
        sql: |
            SELECT *
            FROM networks_network;

extract-redshift
''''''''''''''''

The *extract-redshift* step extracts data from AWS Redshift (the host
and AWS details must be preconfigured in the ~/.dataduct file) into S3.

.. code:: yaml

    -   type: extract-redshift
        schema: dev 
        table: categories

extract-s3
''''''''''

The *extract-s3* step extracts files from a given S3 URI into the output
S3 node.

.. code:: yaml

    -   type: extract-s3
        uri: s3://elasticmapreduce/samples/wordcount/wordSplitter.py

load-redshift
'''''''''''''

The *load-redshift* step loads data from the input nodes to the
specified Redshift table. Before specifying the Redshift table and
schema, the host and AWS details must be preconfigured in the
~/.dataduct file. For example, the following steps will upload a local
file into dev.test\_table

.. code:: yaml

    -   type: extract-local        
        path: examples/resources/test_table1.tsv
      
    -   type: load-redshift        
        schema: dev
        table: test_table  

sql-command
'''''''''''

The *sql-command* step will execute a query in Redshift (the host and
AWS details must be preconfigured in the ~/.dataduct file).

.. code:: yaml

    -   type: sql-command
        command: INSERT INTO dev.test_table VALUES (1, 'hello_etl');

transform
'''''''''

The *transform* step allows you to specify the input node, apply
transformations, and write to a specified output node. The
transformation can be in the form of a script or a UNIX command.

.. code:: yaml

    # Unix Example
    -   type: transform
        command: cp -r $INPUT1_STAGING_DIR/* $OUTPUT1_STAGING_DIR
        input_node:
            step1_a: step2_a
            step1_b: step2_b
        output:
        -   "step2_a"
        -   "step2_b"

    # Script Example
    -   type: transform
        script: examples/scripts/s3_profiler.py
        input_node:
            step2_a: output1
        script_arguments:
        -   "-i=${INPUT1_STAGING_DIR}"
        -   "-o=${OUTPUT1_STAGING_DIR}"
        -   -f 

