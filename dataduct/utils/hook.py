"""Hook helpers
"""
import os
import imp


def default_before_hook(*args, **kwargs):
    """The default before hook, will act like it's not even there
    """
    return args, kwargs


def default_after_hook(result):
    """The default after hook, will act like it's not even there
    """
    return result


def hook(hook_name):
    """The hook decorator creator

    """
    from dataduct.config import Config
    config = Config()
    # Try to load the hook. If it fails, just fallback to default hooks.
    try:
        hook_directory = os.path.expanduser(config.hooks['path'])
        hook_file = os.path.join(hook_directory, hook_name + '.py')

        custom_hook = imp.load_source('custom_hook', hook_file)
        before_hook = custom_hook.before_hook
        after_hook = custom_hook.after_hook
    except Exception:
        before_hook = default_before_hook
        after_hook = default_after_hook

    def hook_deco(func):
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

    return hook_deco
