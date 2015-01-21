#!/usr/bin/env python
"""
Tests for the definition parser functions
"""
import unittest
from nose.tools import raises

from ..etl_actions import read_pipeline_definition
from ...utils.exceptions import ETLInputError


class DefinitionParserTests(unittest.TestCase):
    """Tests for the definition parser.
    """

    @raises(ETLInputError)
    def test_yaml_extension(self):
        """Test if the yaml extension check works correctly
        """
        read_pipeline_definition("name.txt")
