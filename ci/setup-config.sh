#!/bin/bash

set -e

echo "Creating ~/.dataduct directory."
mkdir ~/.dataduct
echo "Succesfully created ~/.dataduct directory."

echo "Creating configuration file."
echo "etl:
  ROLE: DataPipelineDefaultRole
  RESOURCE_ROLE: DataPipelineDefaultResourceRole
  S3_ETL_BUCKET: FILL_ME_IN

ec2:
  CORE_INSTANCE_TYPE: m1.large

emr:
  CLUSTER_AMI: 2.4.7

redshift:
  DATABASE_NAME: FILL_ME_IN
  CLUSTER_ID: FILL_ME_IN
  USERNAME: FILL_ME_IN
  PASSWORD: FILL_ME_IN

mysql:
  DATABASE_KEY:
    HOST: FILL_ME_IN
    USERNAME: FILL_ME_IN
    PASSWORD: FILL_ME_IN" > ~/.dataduct/dataduct.cfg
echo "Successfully created configuration file."
