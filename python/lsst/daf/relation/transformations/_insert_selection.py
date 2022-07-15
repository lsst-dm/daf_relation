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

__all__ = ("InsertSelection",)

from collections.abc import Sequence
from typing import TYPE_CHECKING

from .. import operations
from .._columns import _T
from .._exceptions import EngineError
from .._predicate import Predicate
from .._relation import Relation
from .._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from .._leaf import Leaf


class InsertSelection(RelationVisitor[_T, Relation[_T]]):
    """A `RelationVisitor` that traverses a relation tree to add a join a point
    compatible with the new relation's engine.

    Parameters
    ----------
    predicates : `Sequence` [ `Predicate` ]
        Relation to join into the visited relation's tree.  Must have an engine
        that is contained by the visited relation's engine tree.
    """

    def __init__(self, predicates: Sequence[Predicate[_T]]):
        self.predicates = predicates

    def visit_distinct(self, visited: operations.Distinct[_T]) -> Relation[_T]:
        # Docstring inherited.
        new_base = visited.base.visit(self)
        return operations.Distinct(new_base, visited.unique_keys).assert_checked_and_simplified(
            recursive=False
        )

    def visit_leaf(self, visited: Leaf[_T]) -> Relation[_T]:
        # Docstring inherited.
        return self._fail(visited, self.predicates)

    def visit_join(self, visited: operations.Join[_T]) -> Relation[_T]:
        # Docstring inherited.

        predicate_indices_unmatched = set(range(len(self.predicates)))
        new_relations: list[Relation[_T]] = []
        for i, nested_relation in enumerate(visited.relations):
            matched: list[Predicate[_T]] = []
            unmatched: list[Predicate[_T]] = []
            for n, predicate in enumerate(self.predicates):
                if predicate.columns_required <= nested_relation.columns and any(
                    predicate.supports_engine(engine) for engine in nested_relation.engines
                ):
                    matched.append(predicate)
                    predicate_indices_unmatched.discard(n)
                else:
                    unmatched.append(predicate)
            if matched:
                new_relations.append(nested_relation.visit(InsertSelection(matched)))
            else:
                new_relations.append(nested_relation)
        if predicate_indices_unmatched:
            return self._fail(visited, [self.predicates[n] for n in predicate_indices_unmatched])
        return operations.Join(
            visited.engines.destination, tuple(new_relations), frozenset(visited.conditions)
        ).checked_and_simplified(recursive=False)

    def visit_projection(self, visited: operations.Projection[_T]) -> Relation[_T]:
        # Docstring inherited.
        new_base = visited.base.visit(self)
        return operations.Projection(new_base, frozenset(visited.columns)).assert_checked_and_simplified(
            recursive=False
        )

    def visit_selection(self, visited: operations.Selection[_T]) -> Relation[_T]:
        # Docstring inherited.
        new_base = visited.base.visit(self)
        return operations.Selection(new_base, visited.predicates).assert_checked_and_simplified(
            recursive=False
        )

    def visit_slice(self, visited: operations.Slice[_T]) -> Relation[_T]:
        # Docstring inherited.
        return self._fail(visited, self.predicates)

    def visit_transfer(self, visited: operations.Transfer) -> Relation[_T]:
        # Docstring inherited.
        matched: list[Predicate[_T]] = []
        unmatched: list[Predicate[_T]] = []
        for predicate in self.predicates:
            if predicate.supports_engine(visited.base.engine):
                matched.append(predicate)
            else:
                unmatched.append(predicate)
        new_base = visited.base
        if unmatched:
            new_base = new_base.visit(InsertSelection(unmatched))
        if matched:
            new_base = new_base.selection(*matched)
        return operations.Transfer(new_base, visited.engine)

    def visit_union(self, visited: operations.Union[_T]) -> Relation[_T]:
        # Docstring inherited.
        return operations.Union(
            visited.engines.destination,
            visited.columns,
            tuple(nested_relation.visit(self) for nested_relation in visited.relations),
            unique_keys=frozenset(),
            extra_doomed_by=visited.extra_doomed_by,
        ).assert_checked_and_simplified()

    def _fail(self, relation: Relation[_T], predicates: Sequence[Predicate[_T]]) -> Relation[_T]:
        raise EngineError(
            f"Cannot push selection with predicates {predicates} through {relation} to satisfy engine "
            "consistency."
        )
