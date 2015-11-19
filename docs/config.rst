Config
======

All the dataduct setting are controlled from a single config file that
stores the credentials as well as different settings.

The config file is read from the following places in the specified order
of priority.

1. ``/etc/dataduct.cfg``
2. ``~/.dataduct/dataduct.cfg``
3. ``DATADUCT_CONFIG_PATH`` environment variable

Minimum example config:

.. code:: YAML

    ec2:
        INSTANCE_TYPE: m1.large
        ETL_AMI: ami-05355a6c # Default AMI used by data pipeline - Python 2.6
        SECURITY_GROUP: FILL_ME_IN

    emr:
        MASTER_INSTANCE_TYPE: m1.large
        NUM_CORE_INSTANCES: 1
        CORE_INSTANCE_TYPE: m1.large
        CLUSTER_AMI: 3.1.0

    etl:
        S3_ETL_BUCKET: FILL_ME_IN
        ROLE: FILL_ME_IN
        RESOURCE_ROLE: FILL_ME_IN

Config Parameters
-----------------

Bootstrap
~~~~~~~~~

.. code:: YAML

    bootstrap:
        ec2:
        -   step_type: transform
            command: echo "Welcome to dataduct"
            no_output: true
        emr:
        -   step_type: transform
            command: echo "Welcome to dataduct"
            no_output: true

Bootstrap steps are a chain of steps that should be executed before any
other step in the datapipeline. This can be used to copy files from S3
or install libraries on the resource. At Coursera we use this to
download some binaries from S3 that are required for some of the
transformations.

Note that the EMR bootstrap is only executed on the master node. If you
want to install something on the task nodes then you should use the
bootstrap parameter in the ``emr_cluster_config`` in your datapipeline.

Custom Steps
~~~~~~~~~~~~

::

    custom_steps:
    -   class_name: CustomExtractLocalStep
        file_path: custom_extract_local.py
        step_type: custom-extract-local

Custom steps are steps that are not part of dataduct but are created to
augment the functionality provided by dataduct. At Coursera these are
often Steps that Inherit from the current object but abstract out some
of the functionality so that multiple pipelines don't have to write the
same thing twice.

The file\_path can be an absolute path or a relative path with respect
to the ``CUSTOM_STEPS_PATH`` path defined in the ETL parameter field.
The Step classes are dynamically imported based on the config and
``step-type`` field is the one that is matched when parsing the pipeline
definition.

Database
~~~~~~~~

::

    database:
        permissions:
        -   user: admin
            permission: all
        -   group: consumer_group
            permission: select

Some steps such as ``upsert`` or ``create-load-redshift`` create tables
and grant them appropriate permissions so that one does not have to
create tables prior to running the ETL. The permission is the
``permission`` being granted on the table or view to the ``user`` or
``group``. If both are specified then both the grant statements are
executed.

EC2
~~~

Either Datapipeline can be used for instance management, or you can use an existing
Worker Group. Worker groups have priority over Datapipeline instance management.

Using Datapipeline for instance management:

::

    ec2:
        INSTANCE_TYPE: m1.small
        ETL_AMI: ami-05355a6c # Default AMI used by data pipeline - Python 2.6
        SECURITY_GROUP: FILL_ME_IN

The ec2 config controls the configuration for the ec2-resource started
by the datapipeline. You can override these with ``ec2_resouce_config``
in your pipeline definition for specific pipelines.

Using Worker Groups:

::

    ec2:
        WORKER_GROUP: MY_EC2_WORKER_GROUP_NAME

EMR
~~~

Either Datapipeline can be used for cluster management, or you can use an existing
Worker Group. Worker groups have priority over Datapipeline cluster management.

Using Datapipeline for cluster management:

::

    emr:
        CLUSTER_AMI: 3.1.0
        CLUSTER_TIMEOUT: 6 Hours
        CORE_INSTANCE_TYPE: m1.large
        NUM_CORE_INSTANCES: 1
        HADOOP_VERSION: 2.4.0
        HIVE_VERSION: null
        MASTER_INSTANCE_TYPE: m3.xlarge
        PIG_VERSION: null
        TASK_INSTANCE_BID_PRICE: null
        TASK_INSTANCE_TYPE: m1.large

The emr config controls the configuration for the emr-resource started
by the datapipeline.

Using Worker Groups:

::

    emr:
        WORKER_GROUP: MY_EMR_WORKER_GROUP_NAME

ETL
~~~

::

    etl:
        CONNECTION_RETRIES: 2
        CUSTOM_STEPS_PATH: ~/dataduct/examples/steps
        DAILY_LOAD_TIME: 1
        KEY_PAIR: FILL_ME_IN
        MAX_RETRIES: 2
        NAME_PREFIX: dev
        QA_LOG_PATH: qa
        DP_INSTANCE_LOG_PATH: dp_instances
        DP_PIPELINE_LOG_PATH: dp_pipelines
        DP_QA_TESTS_LOG_PATH: dba_table_qa_tests
        RESOURCE_BASE_PATH: ~/dataduct/examples/resources
        RESOURCE_ROLE: FILL_ME_IN
        RETRY_DELAY: 10 Minutes
        REGION: us-east-1
        ROLE: FILL_ME_IN
        S3_BASE_PATH: dev
        S3_ETL_BUCKET: FILL_ME_IN
        SNS_TOPIC_ARN_FAILURE: null
        SNS_TOPIC_ARN_WARNING: null
        FREQUENCY_OVERRIDE: one-time
        DEPENDENCY_OVERRIDE: false
        HOOKS_BASE_PATH: ~/dataduct/examples/hooks
        TAGS:
            env:
                string: dev
            Name:
                variable: name

This is the core parameter object which controls the ETL at the high
level. The parameters are explained below:

-  ``CONNECTION_RETRIES``: Number of retries for the database
   connections. This is used to eliminate some of the transient errors
   that might occur.
-  ``CUSTOM_STEPS_PATH``: Path to the directory to be used for custom
   steps that are specified using a relative path.
-  ``DAILY_LOAD_TIME``: Default time to be used for running pipelines
-  ``KEY_PAIR``: SSH key pair to be used in both the ec2 and the emr
   resource.
-  ``MAX_RETRIES``: Number of retries for the pipeline activities
-  ``NAME_PREFIX``: Prefix all the pipeline names with this string
-  ``QA_LOG_PATH``: Path prefix for all the QA steps when logging output
   to S3
-  ``DP_INSTANCE_LOG_PATH``: Path prefix for DP instances to be logged
   before destroying
-  ``DP_PIPELINE_LOG_PATH``: Path prefix for DP pipelines to be logged
-  ``DP_QA_TESTS_LOG_PATH``: Path prefix for QA tests to be logged
-  ``RESOURCE_BASE_PATH``: Path to the directory used to relative
   resource paths
-  ``RESOURCE_ROLE``: Resource role needed for DP
-  ``RETRY_DELAY``: Delay between each of activity retires
-  ``REGION``: Region to run the datapipeline from
-  ``ROLE``: Role needed for DP
-  ``S3_BASE_PATH``: Prefix to be used for all S3 paths that are created
   anywhere. This is used for splitting logs across multiple developer
   or across production and dev
-  ``S3_ETL_BUCKET``: S3 bucket to use for DP data, logs, source code
   etc.
-  ``SNS_TOPIC_ARN_FAILURE``: SNS to trigger for failed steps or
   pipelines
-  ``SNS_TOPIC_ARN_WARNING``: SNS to trigger for failed QA checks
-  ``FREQUENCY_OVERRIDE``: Override every frequency given in a pipeline
   with this unless overridden by CLI
-  ``DEPENDENCY_OVERRIDE``: Will ignore the dependency step if set to
   true.
-  ``HOOKS_BASE_PATH``: Path prefix for the hooks directory. For more
   information, see Hooks.
-  ``Tags``: Tags to be added to the pipeline. The first key is the Tag
   to be used, the second key is the type. If the type is string the
   value is passed directly. If the type is variable then it looks up
   the pipeline object for that variable.

Logging
~~~~~~~

::

    logging:
        CONSOLE_DEBUG_LEVEL: INFO
        FILE_DEBUG_LEVEL: DEBUG
        LOG_DIR: ~/.dataduct
        LOG_FILE: dataduct.log

Settings for specifying where the logs should be outputted and debug
levels that should be used in the library code execution.

MySQL
~~~~~

::

    mysql:
        host_alias_1:
            HOST: FILL_ME_IN
            PASSWORD: FILL_ME_IN
            USERNAME: FILL_ME_IN
        host_alias_2:
            HOST: FILL_ME_IN
            PASSWORD: FILL_ME_IN
            USERNAME: FILL_ME_IN

Rds (MySQL) database connections are stored in this parameter. The
pipeline definitions can refer to the host with the host\_alias.
``HOST`` refers to the full db hostname inside AWS.

Redshift
~~~~~~~~

::

    redshift:
        CLUSTER_ID: FILL_ME_IN
        DATABASE_NAME: FILL_ME_IN
        HOST: FILL_ME_IN
        PASSWORD: FILL_ME_IN
        USERNAME: FILL_ME_IN
        PORT: FILL_ME_IN

Redshift database credentials that are used in all the steps that
interact with a warehouse. ``CLUSTER_ID`` is the first word of the
``HOST`` as this is used by ``RedshiftNode`` at a few places to identify
the cluster.

Modes
~~~~~

::

    production:
        etl:
            S3_BASE_PATH: prod

Modes define override settings for running a pipeline. As config is a
singleton we can declare the overrides once and that should update the
config settings across all use cases.

In the example we have a mode called ``production`` in which the
``S3_BASE_PATH`` is overridden to ``prod`` instead of whatever value was
specified in the defaults.

At coursera one of the uses for modes is to change between the dev
redshift cluster to the production one when we deploy a new ETL.
