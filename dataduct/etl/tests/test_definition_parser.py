#!/usr/bin/env python
"""
Tests for the definition parser functions
"""
import unittest
from ..etl_actions import read_pipeline_definition
from ...utils.exceptions import ETLInputError


class DefinitionParserTests(unittest.TestCase):
    """Tests for the definition parser.
    """

    def setUp(self):
        """Fixtures for the definition test
        """
        pass

    def test_yaml_extension(self):
        """Test if the yaml extension check works correctly
        """
        try:
            read_pipeline_definition("name.txt")
            assert False
        except ETLInputError:
            pass
