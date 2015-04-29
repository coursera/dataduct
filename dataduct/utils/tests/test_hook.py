"""Tests for the hooks framework
"""
from unittest import TestCase
from testfixtures import TempDirectory
from nose.tools import eq_

from ..hook import default_before_hook
from ..hook import default_after_hook
from ..hook import hook
from ...config import Config


class TestHooks(TestCase):
    """Tests for the hooks framework
    """

    def setUp(self):
        """Setup hooks framework
        """
        self.temp_directory = TempDirectory()
        config = Config()
        config.etl['HOOKS_BASE_PATH'] = self.temp_directory.path

    def tearDown(self):
        """Teardown temp directory
        """
        self.temp_directory.cleanup()

    @staticmethod
    def test_default_before_hook_simple():
        """Tests that the default before hook works properly (nothing changes)
        """
        args, kwargs = default_before_hook(1, '2', three=3)
        eq_(args, (1, '2'))
        eq_(kwargs, {'three': 3})

    @staticmethod
    def test_default_before_hook_no_params():
        """Tests that the default before hook works properly even with no
        parameters
        """
        args, kwargs = default_before_hook()
        eq_(args, ())
        eq_(kwargs, {})

    @staticmethod
    def test_default_before_hook_only_args():
        """Tests that the default before hook works properly with only args
        """
        args, kwargs = default_before_hook(1, '2')
        eq_(args, (1, '2'))
        eq_(kwargs, {})

    @staticmethod
    def test_default_before_hook_only_kwargs():
        """Tests that the default before hook works properly with only kwargs
        """
        args, kwargs = default_before_hook(one=1, two='2')
        eq_(args, ())
        eq_(kwargs, {'one': 1, 'two': '2'})

    @staticmethod
    def test_default_after_hook_simple():
        """Tests that the default after hook works properly
        """
        result = default_after_hook('test')
        eq_(result, 'test')

    def test_config_not_found_default_hooks(self):
        """Tests that if the hooks directory is not in the config file,
        you don't have any hooks
        """
        hook_file = '\n'.join([
            'def before_hook(number):',
            '    return [number + 1], {}',
            'def after_hook(result):',
            '    return result + 2',
        ])
        self.temp_directory.write('test_hook.py', hook_file)

        config = Config()
        del config.etl['HOOKS_BASE_PATH']

        @hook('test_hook')
        def test_hook(number):
            return number

        eq_(test_hook(1), 1)

    def test_hook_file_not_found_default_hooks(self):
        """Tests that if the hooks file cannot be found, you revert to the
        default hooks
        """
        hook_file = '\n'.join([
            'def before_hook(number):',
            '    return [number + 1], {}',
            'def after_hook(result):',
            '    return result + 2',
        ])
        self.temp_directory.write('unused_hook.py', hook_file)

        @hook('test_hook')
        def test_hook(number):
            return number

        eq_(test_hook(1), 1)

    def test_before_hook_simple(self):
        """Tests that a simple before hook works correctly
        """
        hook_file = '\n'.join([
            'def before_hook(number):',
            '    return [number + 1], {}',
        ])
        self.temp_directory.write('test_hook.py', hook_file)

        @hook('test_hook')
        def test_hook(number):
            return number

        eq_(test_hook(1), 2)

    def test_after_hook_simple(self):
        """Tests that a simple after hook works correctly
        """
        hook_file = '\n'.join([
            'def after_hook(result):',
            '    return result + 2',
        ])
        self.temp_directory.write('test_hook.py', hook_file)

        @hook('test_hook')
        def test_hook(number):
            return number

        eq_(test_hook(1), 3)

    def test_both_before_and_after_hook(self):
        """Tests that you can have an before and an after hook
        """
        hook_file = '\n'.join([
            'def before_hook(number):',
            '    return [number + 1], {}',
            'def after_hook(result):',
            '    return result + 2',
        ])
        self.temp_directory.write('test_hook.py', hook_file)

        @hook('test_hook')
        def test_hook(number):
            return number

        eq_(test_hook(1), 4)

    def test_that_hooks_do_not_leak(self):
        """Test that hooks do not leak to the next hook declaration
        This issue may happen when the import is not cleared properly, and the
        next hook import does not declare all the hooks
        """
        hook_file = '\n'.join([
            'def before_hook(number):',
            '    return [number + 1], {}',
            'def after_hook(result):',
            '    return result + 2',
        ])
        self.temp_directory.write('test_hook.py', hook_file)

        @hook('test_hook')
        def test_hook(number):
            return number

        eq_(test_hook(1), 4)
        second_hook_file = '\n'.join([
            'def before_hook(number):',
            '    return [number + 100], {}',
        ])
        self.temp_directory.write('second_test_hook.py', second_hook_file)

        @hook('second_test_hook')
        def second_test_hook(number):
            return number

        eq_(second_test_hook(1), 101)
