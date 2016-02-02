Installation
============

Installation using pip
----------------------

Dataduct can easily be installed using pip with the following commands.

::

    pip install dataduct

The major dependencies of dataduct are:

-  ``boto`` greater than version 2.34, older versions are missing some
   of the functionality provided by EMR
-  ``PyYAML``
-  ``pandas``
-  ``psycopg2``
-  ``pytimeparse``
-  ``MySQL-python``
-  ``pyparsing``
-  ``testfixtures``

Ensure that a `boto config file <http://boto.cloudhackers.com/en/latest/boto_config_tut.html>`__
containing your AWS credentials is present.

The visualizations are created using:

-  ``graphviz``
-  ``pygraphviz``

Autocomplete for the CLI is supported using:

-  ``argcomplete``

The documentation is created using:

-  ``sphinx``
-  ``sphinx-napolean``
-  ``sphinx_rtd_theme``

Installing in the developer environment
---------------------------------------

1. Clone the Repo
^^^^^^^^^^^^^^^^^

::

    git clone https://github.com/coursera/dataduct.git

2. Update PATH and PYTHONPATH
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add these lines into your ``.bash_profile`` or ``.zshrc`` etc based on
your shell type.

::

    export PYTHONPATH=~/dataduct:$PYTHONPATH
    export PATH=~/dataduct/bin:$PATH

3. Config
^^^^^^^^^

Create a config file. Instructions for this are provided in the config
section.

Setup Autocomplete
------------------

Install argcomplete with ``pip install argcomplete``.

If you're using ``bash`` then add the following to your
``.bash_profile``:

::

    eval "$(register-python-argcomplete dataduct)"

if you're using ``zsh`` then add the following line to your ``.zshrc``:

::

    autoload bashcompinit
    bashcompinit
    eval "$(register-python-argcomplete dataduct)"
