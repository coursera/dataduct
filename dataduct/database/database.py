"""Script containing the database class object
"""
from copy import deepcopy
from .relation import Relation
from .view import View
from .table import Table


class Database(object):
    """Class representing a database
    """

    def __init__(self, relations=None):
        """Constructor for the database class
        """
        self._relations = {}

        if relations:
            for relation in relations:
                self.add_relation(relation)

    def copy(self):
        """Create a copy of the database object
        """
        return deepcopy(self)

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
