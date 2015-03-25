Input and Output Nodes
=======================

In dataduct, data is shared between two activities using S3. After a
step is finished, it saves its output to a file in S3 for successive
steps to read. Input and output nodes abstract this process, they
represent the S3 directories in which the data is stored. A step's input
node determines which S3 file it will read as input, and its output node
determines where it will store its output. In most cases, this
input-output node chain is taken care of by dataduct, but there are
situations where you may want finer control over this process.

Input Nodes
~~~~~~~~~~~

The default behaviour of steps (except Extract- and Check-type steps) is
to link its input node with the preceding step's output node. For
example, in this pipeline snippet

::

    -   step_type: extract-local
        path: data/test_table1.tsv

    -   step_type: create-load-redshift
        table_definition: tables/dev.test_table.sql

the output of the ``extract-local`` step is fed into the
``create-load-redshift`` step, so the pipeline will load the data found
inside ``data/test_table1.tsv`` into ``dev.test_table.sql``. This
behaviour can be made explicit through the ``name`` and ``input_node``
properties.

::

    # This pipeline has the same behaviour as the previous pipeline.
    -   step_type: extract-local
        name: extract_data
        path: data/test_table1.tsv

    -   step_type: create-load-redshift
        input_node: extract_data
        table_definition: tables/dev.test_table.sql

When an input -> output node link is created, implicitly or explicitly,
dependencies are created automatically between the two steps. This
behaviour can be made explicit through the ``depends_on`` property.

::

    # This pipeline has the same behaviour as the previous pipeline.
    -   step_type: extract-local
        name: extract_data
        path: data/test_table1.tsv

    -   step_type: create-load-redshift
        input_node: extract_data
        depends_on: extract_data
        table_definition: tables/dev.test_table.sql

You can use input nodes to communicate between steps that are not next
to each other.

::

    -   step_type: extract-local
        name: extract_data
        path: data/test_table1.tsv

    -   step_type: extract-local
        path: data/test_table2.tsv

    # This step will use the output of the first extract-local step (test_table1.tsv)
    -   step_type: create-load-redshift
        input_node: extract_data
        table_definition: tables/dev.test_table.sql

Without the use of ``input_node``, the ``create-load-redshift`` step
would have used the data from ``test_table2.tsv`` instead.

You can also use input nodes to reuse the output of a step.

::

    -   step_type: extract-local
        name: extract_data
        path: data/test_table1.tsv

    -   step_type: create-load-redshift
        input_node: extract_data
        table_definition: tables/dev.test_table1.sql

    -   step_type: create-load-redshift
        input_node: extract_data
        table_definition: tables/dev.test_table2.sql

Sometimes, you may not want a step to have any input nodes. You can
specify this by writing ``input_node: []``.

::

    -   step_type: extract-local
        name: extract_data
        path: data/test_table1.tsv

    # This step will not receive any input data
    -   step_type: transform
        input_node: []
        script: scripts/example_script.py

If you are running your own script (e.g. through the Transform step),
the input node's data can be found in the directory specified by the
``INPUT1_STAGING_DIR`` enviroment variable.

::

    -   step_type: extract-local
        name: extract_data
        path: data/test_table1.tsv

    # manipulate_data.py takes in the input directory as a script argument and
    # converts the string into the enviroment variable.
    -   step_type: transform
        script: scripts/manipulate_data.py
        script_arguments:
        -   --input=INPUT1_STAGING_DIR

Output Nodes
~~~~~~~~~~~~

Dataduct usually handles a step's output nodes automatically, saving the
file into a default path in S3. You can set the default path through
your dataduct configuration file. However, some steps also have an
optional ``output_path`` property, allowing you to choose an S3
directory to store the step's output.

Transform Step and Output Nodes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Transform steps allow you to run your own scripts. If you want to save
the results of your script, you can store data into the output node by
writing to the directory specified by the ``OUTPUT1_STAGING_DIR`` enviroment
variable.

::

    # generate_data.py takes in the output directory as a script argument and
    # converts the string into the enviroment variable.
    -   step_type: transform
        script: scripts/generate_data.py
        script_arguments:
        -   --output=OUTPUT1_STAGING_DIR

    -   step_type: create-load-redshift
        table_definition: tables/dev.test_table.sql

You may wish to output more than one set of data for multiple proceeding
steps to use. You can do this through the ``output_node`` property.

::

    -   step_type: transform
        script: scripts/generate_data.py
        script_arguments:
        -   --output=OUTPUT1_STAGING_DIR
        output_node:
        -   foo_data
        -   bar_data

    -   step_type: create-load-redshift
        input_node: foo_data
        table_definition: tables/dev.test_table1.sql

    -   step_type: create-load-redshift
        input_node: bar_data
        table_definition: tables/dev.test_table2.sql

In this case, the script must save data to subdirectories with names
matching the output nodes. In the above example, ``generate_data.py``
must save data in ``OUTPUT1_STAGING_DIR/foo_data`` and
``OUTPUT1_STAGING_DIR/bar_data`` directories. If the subdirectory and
output node names are mismatched, the output nodes will not be generated
correctly.
