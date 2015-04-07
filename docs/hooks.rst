Hooks
=====

Dataduct has some endpoints you can use to execute python scripts before and
after certain events when using the CLI and library locally.

Available Hooks
~~~~~~~~~~~~~~~

-  ``activate_pipeline``, which hooks onto the ``activate_pipeline`` function in
   ``dataduct.etl.etl_actions``.
-  ``connect_to_redshift``, which hooks onto the ``redshift_connection`` function in
   ``dataduct.data_access``.

Creating a hook
~~~~~~~~~~~~~~~

Dataduct tries to find available hooks by searching in the directory specified
by the ``HOOKS_BASE_PATH`` config variable in the ``etl`` section, matching them
by their filename. For example, a hook for the ``activate_pipeline``
endpoint would saved as ``activate_pipeline.py`` in that directory.

Each hook has two endpoints: ``before_hook`` and ``after_hook``. To implement
one of these endpoints, you declare them as functions inside the hook. You may
implement only one or both endpoints per hook.

``before_hook`` is called before the hooked function is executed. The parameters
passed into the hooked function will also be passed to the ``before_hook``.
The ``before_hook`` is designed to allow you to manipulate the arguments of
the hooked function before it is called. At the end of the ``before_hook``,
return the ``args`` and ``kwargs`` of the hooked function as a tuple.

Example ``before_hook``:

.. code:: python

    # hooked function signature:
    # def example(arg_one, arg_two, arg_three='foo')

    def before_hook(arg_one, arg_two, arg_three='foo'):
        return [arg_one + 1, 'hello world'], {'arg_three': 'bar'}

``after_hook`` is called after the hooked function is executed. The result of the
hooked function is passed into ``after_hook`` as a single parameter.
The ``after_hook`` is designed to allow you to access or manipulate the result of
the hooked function. At the end of the ``after_hook``, return the (modified)
result of the hooked function.

Example ``after_hook``:

.. code:: python

    # hooked function result: {'foo': 1, 'bar': 'two'}

    def after_hook(result):
        result['foo'] = 2
        result['bar'] = result['bar'] + ' three'
        return result
