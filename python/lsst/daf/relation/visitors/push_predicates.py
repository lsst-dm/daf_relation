# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("PushPredicates",)

from typing import TYPE_CHECKING, AbstractSet

from .. import operations
from .._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._predicate import Predicate
    from .._leaf import Leaf
    from .._relation import Relation


class PushPredicates(RelationVisitor[_T, Relation[_T]]):
    def __init__(self, predicates: AbstractSet[Predicate[_T]]):
        self.predicates = predicates

    def visit_leaf(self, visited: Leaf[_T]) -> Relation[_T]:
        return operations.Selection(visited, frozenset(self.predicates))

    def visit_join(self, visited: operations.Join[_T]) -> Relation[_T]:
        remaining_predicates = set(self.predicates)
        new_relations: list[Relation[_T]] = []
        for relation in visited.relations:
            matches = {p for p in remaining_predicates if relation.columns >= p.columns_required}
            if matches:
                relation = relation.visit(PushPredicates[_T](matches))
                remaining_predicates.difference_update(matches)
            new_relations.append(relation)
        if remaining_predicates == self.predicates:
            base = visited
        else:
            base = operations.Join(visited.engine, tuple(new_relations), visited.conditions)
        if remaining_predicates:
            return operations.Selection(base, frozenset(remaining_predicates))
        else:
            return base

    def visit_projection(self, visited: operations.Projection[_T]) -> Relation[_T]:
        return operations.Projection(visited.base.visit(self), visited.columns)

    def visit_selection(self, visited: operations.Selection[_T]) -> Relation[_T]:
        new_base = visited.base.visit(self)
        match new_base:
            case operations.Selection(base=nested_base, predicates=nested_predicates):
                return operations.Selection(nested_base, nested_predicates | visited.predicates)
        return operations.Selection(new_base, visited.predicates)

    def visit_slice(self, visited: operations.Slice[_T]) -> Relation[_T]:
        return operations.Selection(visited, frozenset(self.predicates))

    def visit_transfer(self, visited: operations.Transfer[_T]) -> Relation[_T]:
        matches = {p for p in self.predicates if visited.base.engine in p.state}
        if matches:
            base = operations.Transfer(visited.base.visit(PushPredicates(matches)), visited.engine)
        else:
            base = visited
        remaining = self.predicates - matches
        if remaining:
            return operations.Selection(base, frozenset(remaining))
        else:
            return base

    def visit_union(self, visited: operations.Union[_T]) -> Relation[_T]:
        new_relations = tuple(relation.visit(self) for relation in visited.relations)
        return operations.Union(
            visited.engine, visited.columns, new_relations, visited.unique_keys, visited.extra_doomed_by
        )
