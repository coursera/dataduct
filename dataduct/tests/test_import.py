"""Tests for dependencies
"""
from unittest import TestCase


class TestImports(TestCase):
    """Tests for dependencies
    """
    @staticmethod
    def test_boto():
        """Testing boto
        """
        print 'Trying to import boto'
        import boto

    @staticmethod
    def test_mysqldb():
        """Testing MySQLdb
        """
        print 'Trying to import MySQLdb'
        import MySQLdb

    @staticmethod
    def test_pandas():
        """Testing pandas
        """
        print 'Trying to import pandas'
        import pandas
        print pandas.io.sql

    @staticmethod
    def test_psycopg2():
        """Testing psycopg2
        """
        print 'Trying to import psycopg2'
        import psycopg2

    @staticmethod
    def test_pygraphviz():
        """Testing pygraphviz
        """
        print 'Trying to import pygraphviz'
        import pygraphviz

    @staticmethod
    def test_pyparsing():
        """Testing pyparsing
        """
        print 'Trying to import pyparsing'
        import pyparsing

    @staticmethod
    def test_pyyaml():
        """Testing PyYAML
        """
        print 'Trying to import pyyaml'
        import yaml

    @staticmethod
    def test_setuptools():
        """Testing setuptools
        """
        print 'Trying to import setuptools'
        import setuptools

    @staticmethod
    def test_sphinx_rtd_theme():
        """Testing sphinx_rtd_theme
        """
        print 'Trying to import sphinx_rtd_theme'
        import sphinx_rtd_theme

    @staticmethod
    def test_testfixtures():
        """Testing testfixtures
        """
        print 'Trying to import testfixtures'
        import testfixtures
