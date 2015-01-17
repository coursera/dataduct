"""Script containing the database class object
"""
from copy import deepcopy

from .relation import Relation
from .view import View
from .table import Table
from .sql import SqlScript

from ..utils.helpers import atmost_one
from ..utils.helpers import parse_path

from ..utils.exceptions import DatabaseInputError

import logging
logger = logging.getLogger(__name__)

class Database(object):
    """Class representing a database
    """

    def __init__(self, relations=None, files=None):
        """Constructor for the database class
        """
        self._relations = {}

        if not atmost_one(relations, files):
            raise ValueError('Only one of relations and files should be given')

        if files:
            relations = self._initialize_relations(files)

        if relations:
            for relation in relations:
                self.add_relation(relation)

    def copy(self):
        """Create a copy of the database object
        """
        return deepcopy(self)

    @staticmethod
    def _initialize_relations(files):
        """Read the files and create relations from the files
        """
        relations = []
        for filename in files:
            with open(parse_path(filename)) as f:
                script = SqlScript(f.read())
                if script.creates_table():
                    relations.append(Table(script))
                elif script.creates_view():
                    relations.append(View(script))
                else:
                    raise ValueError('File does not create a relation')
        return relations

    def add_relation(self, relation):
        """Add a relation, only if its name is not already used.
        """
        assert isinstance(relation, Relation), 'Input should be a relation'
        if relation.full_name in self._relations:
            raise ValueError(
                'Relation %s already added to database' % relation.full_name)

        self._relations[relation.full_name] = relation

    def relations(self):
        """Unsorted list of relations of the database
        """
        return self._relations.values()

    def relation(self, relation_name):
        """Get the relation with the given name
        """
        return self._relations.get(relation_name, None)

    @property
    def num_views(self):
        """The number of views in the database
        """
        return len([a for a in self.relations() if isinstance(a, View)])

    @property
    def num_tables(self):
        """The number of tables in the database
        """
        return len([a for a in self.relations() if isinstance(a, Table)])

    def has_cycles(self, relation=None, visited=None):
        """Check if the database has no circular dependencies
        """
        if visited is None:
            visited = list()

        if relation:
            # Don't include table as own dependency, ignore references not in DB
            relations_to_check = [
                self.relation(x) for x in relation.dependencies
                if x != relation and self.relation(x) is not None]
        else:
            relations_to_check = self._relations.values()

        for relation in relations_to_check:
            if relation.full_name in visited:
                return True
            # Make a copy for immutability
            visited = deepcopy(visited)
            visited.append(relation.full_name)
            if self.has_cycles(relation, visited):
                return True
        return False

    def sorted_relations(self):
        """Topological sort of the relations for dependency management
        """
        if self.has_cycles():
            print 'Warning: database has cycles'

        sorted_relations = []
        graph = dict((x.full_name, x.dependencies) for x in self.relations())

        # Run until the unsorted graph is empty
        while graph:
            acyclic = False
            for relation_name, dependencies in graph.items():
                for dependency in dependencies:
                    if dependency in graph:
                        break
                else:
                    acyclic = True
                    graph.pop(relation_name)
                    sorted_relations.append(self.relation(relation_name))

            if not acyclic:
                raise RuntimeError("A cyclic dependency occurred")
        return sorted_relations

    def relations_script(self, function_name, **kwargs):
        """SQL Script for all the relations of the database
        """
        result = SqlScript()
        for relation in self.sorted_relations():
            func = getattr(relation, function_name)
            result.append(func(**kwargs))
        return result

    def grant_relations_script(self):
        """SQL Script for granting permissions all the relations of the database
        """
        return self.relations_script('grant_script')

    def create_relations_script(self, grant_permissions=True):
        """SQL Script for creating all the relations of the database
        """
        return self.relations_script(
            'create_script', grant_permissions=grant_permissions)

    def drop_relations_script(self):
        """SQL Script for dropping all the relations for the database
        """
        return self.relations_script('drop_script')

    def recreate_relations_script(self, grant_permissions=True):
        """SQL Script for recreating all the relations of the database
        """
        return self.relations_script(
            'recreate_script', grant_permissions=grant_permissions)

    def recreate_table_dependencies(self, table_name):
        """Recreate the dependencies for a particular table from the database
        """
        result = SqlScript()
        for relation in self.relations():
            if relation.full_name == table_name:
                # Continue as cannnot be dependecy of self
                continue

            if isinstance(relation, Table):
                # Recreate foreign key relations
                for column_names, ref_name, ref_columns in \
                        relation.forign_key_references():
                    if ref_name == table_name:
                        result.append(
                            relation.foreign_key_reference_script(
                                source_columns=column_names,
                                reference_name=ref_name,
                                reference_columns=ref_columns))

            if isinstance(relation, View):
                # Recreate view if pointing to table
                if table_name in relation.dependencies:
                    result.append(relation.recreate_script())
        return result

    def _make_node_label(self, relation):
        """Create the html table layout for graph nodes
        """
        html_lines = ['<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0">']
        html_lines += ['<TR><TD BGCOLOR="grey"><U>' + relation.full_name +
                       '</U></TD></TR>']
        for col in sorted(relation.columns, key=lambda x: x.position):
            col_name = col.name + (' PK' if col.primary else '')
            html_lines += ['<TR><TD ALIGN="left" PORT="' + col.name + '">' +
                           col_name + '</TD></TR>']
        html_lines += ['</TABLE>>']

        return '\n'.join(html_lines)

    def visualize(self, filename=None):
        """Visualize databases
        """
        # Import pygraphviz for plotting the graphs
        try:
            import pygraphviz
        except ImportError:
            raise ImportError('Install pygraphviz for visualizing databases')

        if filename is None:
            raise DatabaseInputError(
                'Filename must be provided for visualization')

        logger.info('Creating a visualization of the database')
        graph = pygraphviz.AGraph(
            name='database', label='database')

        # Add nodes
        for relation in self.relations():
            if isinstance(relation, Table):
                graph.add_node(relation.full_name)
                node = graph.get_node(relation.full_name)
                node.attr['label'] = self._make_node_label(relation)
                node.attr['shape'] = 'none'

        # Add edges
        for relation in self.relations():
            if isinstance(relation, Table):
                for cols, ref_table_name, ref_col_names in \
                        relation.foreign_key_references():
                    # ref_name = ref_table_name + \
                    #     ':' + ref_col_names
                    graph.add_edge(relation.full_name, ref_table_name)
                    # graph.add_edge(t.full_name + ":" + cols[0], ref_name)

        # Plotting the graph with dot layout
        graph.layout(prog='dot')
        graph.draw(filename)
