"""Hook framework in dataduct.

To make a function hookable, add the hook decorator like so:

@hook('hook_name')
def function():
    ...
"""
import os
import imp
import sys

from .helpers import parse_path


def default_before_hook(*args, **kwargs):
    """The default before hook, will act like it's not even there
    """
    return args, kwargs


def default_after_hook(result):
    """The default after hook, will act like it's not even there
    """
    return result


def get_hooks(hook_name):
    """Returns the before hook and after hook (in a tuple) for a particular
    hook name
    """
    from dataduct.config import Config
    config = Config()

    if 'HOOKS_BASE_PATH' not in config.etl:
        return default_before_hook, default_after_hook

    hook_file = parse_path(hook_name + '.py', 'HOOKS_BASE_PATH')
    if not os.path.isfile(hook_file):
        return default_before_hook, default_after_hook

    # Delete the previous custom hook, so the imports are not merged.
    if 'custom_hook' in sys.modules:
        del sys.modules['custom_hook']

    # Get the hook functions, falling back to the default hooks
    custom_hook = imp.load_source('custom_hook', hook_file)
    before_hook = getattr(custom_hook, 'before_hook', default_before_hook)
    after_hook = getattr(custom_hook, 'after_hook', default_after_hook)

    return before_hook, after_hook


def hook(hook_name):
    """The hook decorator creator
    """
    before_hook, after_hook = get_hooks(hook_name)

    def hook_decorator(func):
        """The hook decorator
        """
        def function_wrapper(*args, **kwargs):
            """The hook wrapper for the function
            """
            new_args, new_kwargs = before_hook(*args, **kwargs)
            result = func(*new_args, **new_kwargs)
            new_result = after_hook(result)
            return new_result

        return function_wrapper

    return hook_decorator
