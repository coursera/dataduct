Installation
~~~~~~~~~~~~

Install the dataduct package using pip

::

    pip install dataduct

**Dependencies**

dataduct currently has the following dependencies:

- boto >= 2.32.0
- yaml

We have tried some older versions of boto with the problem being support
some functionality around EMR that will be used in the later versions of
dataduct.

**Setup Configuration**

Setup the configuration file to set the credentials and default values
for various parameters passed to datapipeline.
