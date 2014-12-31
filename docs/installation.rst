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
      ROLE: FILL_ME_IN
      RESOURCE_ROLE: FILL_ME_IN
      INSTANCE_TYPE: m1.large
      ETL_AMI: ami-05355a6c # Default AMI used by data pipeline
      KEY_PAIR: FILL_ME_IN
      SECURITY_GROUP: FILL_ME_IN

    emr:
      NUM_CORE_INSTANCES: 3
      CORE_INSTANCE_TYPE: m1.large
      TASK_INSTANCE_BID_PRICE: null  # null if we want it to be None
      TASK_INSTANCE_TYPE: m1.large
      MASTER_INSTANCE_TYPE: m1.large
      CLUSTER_TIMEOUT: 6 Hours
      HADOOP_VERSION: null
      HIVE_VERSION: null
      PIG_VERSION: null
      CLUSTER_AMI: 2.4.7

    redshift:
      DATABASE_NAME: FILL_ME_IN
      CLUSTER_ID: FILL_ME_IN
      USERNAME: FILL_ME_IN
      PASSWORD: FILL_ME_IN

    mysql:
      DATABASE_KEY:
        HOST: FILL_ME_IN,
        USERNAME: FILL_ME_IN,
        PASSWORD: FILL_ME_IN

    etl:
      RETRY_DELAY: 10 Minutes
      MAX_RETRIES: 0
      S3_ETL_BUCKET: FILL_ME_IN
      SNS_TOPIC_ARN_FAILURE: FILL_ME_IN
      SNS_TOPIC_ARN_WARNING: FILL_ME_IN
      DAILY_LOAD_TIME: 1  # run at 1AM UTC

    bootstrap:
      - step_type: transform
        input_node: []
        command: whoami >> ${OUTPUT1_STAGING_DIR}/output.txt
        resource: FILL_ME_IN
        name: bootstrap_transform
