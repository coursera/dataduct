Installation
~~~~~~~~~~~~

Install the dataduct package using pip

::

    pip install dataduct

**Dependencies**

dataduct currently has the following dependencies: - boto >= 2.32.0 -
yaml

We have tried some older versions of boto with the problem being support
some functionality around EMR that will be used in the later versions of
dataduct.

**Setup Configuration**

Setup the configuration file to set the credentials and defaul values
for various parameters passed to datapipeline. Copy the config template
from https://github.com/coursera/dataduct/../example\_config and write
it to ``~/.dataduct`` or ``/etc/.dataduct``. You can also set an
environment variable pointing to the config file location by setting the
``DATADUCT_PATH`` variable.

*Config file template:*

::

    # Constants that are used across the dataduct library

    ec2:
      DEFAULT_ROLE: FILL_ME_IN
      DEFAULT_RESOURCE_ROLE: FILL_ME_IN
      DEFAULT_EC2_INSTANCE_TYPE: m1.large
      ETL_AMI: FILL_ME_IN
      KEY_PAIR: FILL_ME_IN
      SECURITY_GROUP: FILL_ME_IN

    emr:
      DEFAULT_NUM_CORE_INSTANCES: 3
      DEFAULT_CORE_INSTANCE_TYPE: m1.large
      DEFAULT_TASK_INSTANCE_BID_PRICE: null  # null if we want it to be None
      DEFAULT_TASK_INSTANCE_TYPE: m1.large
      DEFAULT_MASTER_INSTANCE_TYPE: m1.large
      DEFAULT_CLUSTER_TIMEOUT: 6 Hours
      DEFAULT_HADOOP_VERSION: null
      DEFAULT_HIVE_VERSION: null
      DEFAULT_PIG_VERSION: null
      DEFAULT_CLUSTER_AMI: 2.4.7

    redshift:
      REDSHIFT_DATABASE_NAME: FILL_ME_IN
      REDSHIFT_CLUSTER_ID: FILL_ME_IN
      REDSHIFT_USERNAME: FILL_ME_IN
      REDSHIFT_PASSWORD: FILL_ME_IN

    mysql:
      DATABASE_KEY:
        HOST: FILL_ME_IN,
        USERNAME: FILL_ME_IN,
        PASSWORD: FILL_ME_IN

    etl:
      RETRY_DELAY: 10 Minutes
      DEFAULT_MAX_RETRIES: 0
      ETL_BUCKET: FILL_ME_IN
      DATA_PIPELINE_TOPIC_ARN: FILL_ME_IN
      DAILY_LOAD_TIME: 1  # run at 1AM UTC

    bootstrap:
      - step_type: transform
        input_node: []
        command: whoami >> ${OUTPUT1_STAGING_DIR}/output.txt
        resource: FILL_ME_IN
        name: bootstrap_transform
