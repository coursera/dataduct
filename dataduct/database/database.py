"""Script containing the database class object
"""
from copy import deepcopy

from .relation import Relation
from .view import View
from .table import Table
from .sql import SqlScript

from ..utils.helpers import atmost_one
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
            with open(filename) as f:
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
            visited_copy = deepcopy(visited)
            visited_copy.append(relation.full_name)
            if self.has_cycles(relation, visited_copy):
                return True
        return False

    def sorted_relations(self):
        """Topological sort of the relations for dependency management
        """
        if self.has_cycles():
            logger.warning('Database has cycles')

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

    def recreate_table_dependencies(self, table_name, grant_permissions=True):
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
                        relation.foreign_key_references():
                    if ref_name == table_name:
                        result.append(
                            relation.foreign_key_reference_script(
                                source_columns=column_names,
                                reference_name=ref_name,
                                reference_columns=ref_columns))

            if isinstance(relation, View):
                # Recreate view if pointing to table
                if table_name in relation.dependencies:
                    result.append(relation.recreate_script(
                        grant_permissions=grant_permissions))
        return result

    @staticmethod
    def _make_node_label(relation):
        """Create the table layout for graph nodes
        """
        columns = list()
        row = '<TR><TD ALIGN="left" PORT="{col_name}">{col_name}{pk}</TD></TR>'
        for column in sorted(relation.columns(), key=lambda x: x.position):
            columns.append(row.format(col_name=column.name,
                                      pk=' (PK)' if column.primary else ''))

        layout = ('<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0">\n'
                  '<TR><TD BGCOLOR="lightblue">{table_name}</TD></TR>\n'
                  '{columns}</TABLE>>').format(table_name=relation.full_name,
                                               columns='\n'.join(columns))
        return layout

    def visualize(self, filename=None, tables_to_show=None):
        """Visualize databases and create an er-diagram

        Args:
            filename(str): filepath for saving the er-diagram
            tables_to_show(list): A list of tables to actually visualize.
                Tables not included in this list will not be visualized,
                but their foreign keys will be visualize if it refers to a
                table in this list
        """
        # Import pygraphviz for plotting the graphs
        try:
            import pygraphviz
        except ImportError:
            logger.error('Install pygraphviz for visualizing databases')
            raise

        if filename is None:
            raise DatabaseInputError(
                'Filename must be provided for visualization')

        logger.info('Creating a visualization of the database')
        graph = pygraphviz.AGraph(name='Database', label='Database')

        tables = [r for r in self.relations() if isinstance(r, Table)]
        if tables_to_show is None:
            tables_to_show = [table.full_name for table in tables]

        # Add nodes
        for table in tables:
            if table.full_name in tables_to_show:
                graph.add_node(table.full_name, shape='none',
                               label=self._make_node_label(table))

        # Add edges
        for table in tables:
            for cols, ref_table, ref_cols in table.foreign_key_references():
                if table.full_name in tables_to_show or \
                        ref_table in tables_to_show:
                    graph.add_edge(
                        ref_table,
                        table.full_name,
                        tailport=ref_cols[0],
                        headport=cols[0],
                        dir='both',
                        arrowhead='crow',
                        arrowtail='dot',
                    )

        # Plotting the graph with dot layout
        graph.layout(prog='dot')
        graph.draw(filename)
