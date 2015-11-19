Commands
========

Pipeline Commands
-----------------

Commands used to manipulate pipelines.

Usage:

::

    dataduct pipeline [-h] {create,validate,activate,visualize}

Arguments:

-  ``-h, --help``: Show help message and exit.

Create, Validate, Activate
^^^^^^^^^^^^^^^^^^^^^^^^^^

Usage:

::

    dataduct pipeline {create,validate,activate}
        [-h] [-m MODE] [-f] [-t TIME_DELTA] [-b] [--frequency FREQUENCY]
        pipeline_definitions [pipeline_definitions ...]

-  ``create``: Creates a pipeline locally.

-  ``validate``: Creates a pipeline on Amazon DataPipeline and validates the pipeline.

-  ``activate``: Creates and validates the pipeline, then activates the pipeline.


Arguments:

-  ``-h, --help``: Show help message and exit.
-  ``-m MODE, --mode MODE``: Mode or config variables to use. e.g. ``-m production``
-  ``-f, --force``: Destroy previous version of this pipeline, if they exist.
-  ``-t TIME_DELTA, --time_delta TIME_DELTA``: Timedelta the pipeline by x time difference. e.g. ``-t "1 day"``
-  ``-b, --backfill``: Indicates that the timedelta supplied is for a backfill.
-  ``-frequency FREQUENCY``: Frequency override for the pipeline.
-  ``pipeline_definitions``: The YAML defintions of the pipeline.

Visualize
^^^^^^^^^

Visualizes the pipeline into a PNG file.

Usage:

::

    dataduct pipeline visualize [-h] [-m MODE] [--activities_only]
                                filename pipeline_definitions
                                [pipeline_definitions ...]

Arguments:

-  ``-h --help``: Show help message and exit.
-  ``-m MODE, --mode MODE``: Mode or config variables to use. e.g. ``-m production``
-  ``--activities_only``: Visualize pipeline activities only.
-  ``filename``: The filename for saving the visualization.
-  ``pipeline_definitions``: The YAML defintions of the pipeline.

Database Commands
-----------------

Commands used to generate SQL for various actions.

Create, Drop, Grant, Recreate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  ``create``: Generates SQL to create relations.
-  ``drop``: Generates SQL to drop relations.
-  ``grant``: Generates SQL to grant permissions.
-  ``recreate``: Generates SQL to recreate relations.

Usage:

::

    dataduct database {create,drop,grant,recreate}
        [-h] [-m MODE] table_definitions [table_definitions ...]

Arguments:

-  ``-h, --help``: Show help message and exit.
-  ``-m MODE, --mode MODE``: Mode or config variables to use. e.g. ``-m production``
-  ``table_definitions``: The SQL definitions of the relations.

Visualize
^^^^^^^^^

Creates an entity relationship diagram of the tables as a PNG file.

Usage:

::

    dataduct database visualize [-h] [-m MODE]
                                filename table_definitions
                                [table_definitions ...]

Arguments:

-  ``-h, --help``: Show help message and exit.
-  ``-m MODE, --mode MODE``: Mode or config variables to use. e.g. ``-m production``
-  ``filename``: The filename for saving the visualization.
-  ``table_definitions``: The SQL definitions of the tables.

Configuration Commands
----------------------

Commands used to synchronize the config file from/to Amazon S3.

Sync To S3
^^^^^^^^^^
Uploads the local config file to S3. Will automatically detect the location of the config file.See the config documentation for more information.

Usage:

::

    dataduct config sync_to_s3 [-h] [-m MODE]

Arguments:

-  ``-h, --help``: Show help message and exit.
-  ``-m MODE, --mode MODE``: Mode or config variables to use. e.g. ``-m production``

Sync From S3
^^^^^^^^^^^^
Downloads the local config file from S3 and saves it to a file.

Usage:

::

    dataduct config sync_from_s3 [-h] [-m MODE] filename

Arguments:

-  ``-h, --help``: Show help message and exit.
-  ``-m MODE, --mode MODE``: Mode or config variables to use. e.g. ``-m production``
-  ``filename``: The filename for saving the config file.

SQL Shell Commands
----------------------

Commands used to connect to either MySQL or Redshift via the terminal.

MySQL
^^^^^^^^^^
Connects to a MySQL database using a host alias.

Usage:

::

    dataduct sql_shell mysql [-h] [-m MODE] host_alias

Arguments:

-  ``-h, --help``: Show help message and exit.
-  ``-m MODE, --mode MODE``: Mode or config variables to use. e.g. ``-m production``
-  ``host_alias``: The host alias of the database to connect to.

Redshift
^^^^^^^^^^
Connects to the Redshift database specified in Dataduct configs.

Usage:

::

    dataduct sql_shell redshift [-h] [-m MODE]

Arguments:

-  ``-h, --help``: Show help message and exit.
-  ``-m MODE, --mode MODE``: Mode or config variables to use. e.g. ``-m production``
