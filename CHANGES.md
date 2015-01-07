# Changes in dataduct

### 0.1.0
- Initial version of the dataduct library released
- Support for the following steps:
    - emr_streaming
    - extract-local
    - extract-s3
    - extract-rds
    - extract-redshift
    - load-redshift
    - sql-command
    - transform
- Examples and documentation added for all the steps

### 0.2.0
- Support for custom steps
- Pipeline dependency step
- Reduce verbosity of imports
- Step parsing is isolated in steps
- More examples for steps
- QA step functions added
- Visualization of pipelines
- Sync config with S3
- Config overides with modes
- Rename keywords and safe config failure handling
- MySQL and Redshift connection support
- EMR Streaming support with hadoop 2
- Custom EMR job step
- Support for input_path to steps to directly create S3Nodes
- Transform step to support directory based installs
- Exceptions cleanup
- Read the docs support
