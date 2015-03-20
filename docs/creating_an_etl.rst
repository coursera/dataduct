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
    -   step_type: extract-local
        path: data/word_data.txt

    -   step_type: emr-streaming
        mapper: scripts/word_mapper.py
        reducer: scripts/word_reducer.py

    -   step_type: transform
        script: scripts/s3_profiler.py
        script_arguments:
        -   --input=INPUT1_STAGING_DIR
        -   --output=OUTPUT1_STAGING_DIR
        -   -f


Header Information
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
