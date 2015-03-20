.. dataduct documentation master file, created by
   sphinx-quickstart on Mon Nov 10 17:50:14 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Dataduct
========

    Dataduct - DataPipeline for humans

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


Contents:

.. toctree::
   :maxdepth: 2

   introduction
   installation
   config
   creating_an_etl
   steps
   input_output
   dataduct

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

