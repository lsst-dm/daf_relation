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

__all__ = ("InsertJoin",)

from collections.abc import Set
from typing import TYPE_CHECKING

from .. import operations
from .._columns import _T
from .._exceptions import EngineError
from .._join_condition import JoinCondition
from .._relation import Relation
from .._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from .._leaf import Leaf


class InsertJoin(RelationVisitor[_T, Relation[_T]]):
    """A `RelationVisitor` that traverses a relation tree to add a join a point
    compatible with the new relation's engine.

    Parameters
    ----------
    relation : `Relation`
        Relation to join into the visited relation's tree.  Must have an engine
        that is contained by the visited relation's engine tree.
    conditions : `~collections.abc.Set` [ `JoinCondition` ]
        Special conditions for the join.  Must match ``relation`` and the
        visited relation, but the flip order does not matter.
    """

    def __init__(self, relation: Relation[_T], conditions: Set[JoinCondition[_T]]):
        self.relation = relation
        self.conditions = conditions

    def visit_distinct(self, visited: operations.Distinct[_T]) -> Relation[_T]:
        # Docstring inherited.
        return self._fail(visited)

    def visit_leaf(self, visited: Leaf[_T]) -> Relation[_T]:
        # Docstring inherited.
        return self._fail(visited)

    def visit_join(self, visited: operations.Join[_T]) -> Relation[_T]:
        # Docstring inherited.
        for i, nested_relation in enumerate(visited.relations):
            if (
                self.relation.engine.tag in nested_relation.engine
                and JoinCondition.find_matching(
                    nested_relation.columns, self.relation.columns, self.conditions
                )
                == self.conditions
            ):
                new_relations = list(visited.relations)
                new_relations[i] = nested_relation.visit(self)
                return operations.Join(
                    visited.engine.tag, tuple(new_relations), frozenset(visited.conditions)
                ).checked_and_simplified(recursive=False)
        return self._fail(visited)

    def visit_projection(self, visited: operations.Projection[_T]) -> Relation[_T]:
        # Docstring inherited.
        new_base = visited.base.visit(self)
        return operations.Projection(new_base, frozenset(visited.columns)).assert_checked_and_simplified(
            recursive=False
        )

    def visit_selection(self, visited: operations.Selection[_T]) -> Relation[_T]:
        # Docstring inherited.
        new_base = visited.base.visit(self)
        return operations.Selection(new_base, frozenset(visited.predicates)).assert_checked_and_simplified(
            recursive=False
        )

    def visit_slice(self, visited: operations.Slice[_T]) -> Relation[_T]:
        # Docstring inherited.
        return self._fail(visited)

    def visit_transfer(self, visited: operations.Transfer) -> Relation[_T]:
        # Docstring inherited.
        if visited.base.engine.tag == self.relation.engine.tag:
            new_base = visited.base.join(self.relation, conditions=self.conditions)
        else:
            new_base = visited.base.visit(self)
        return operations.Transfer(new_base, visited.engine.tag)

    def visit_union(self, visited: operations.Union[_T]) -> Relation[_T]:
        # Docstring inherited.
        new_relations: list[Relation[_T]] = []
        for nested_relation in visited.relations:
            if self.relation.engine.tag in nested_relation.engine:
                new_relations.append(nested_relation.visit(self))
            else:
                return self._fail(visited)
        return operations.Union(
            visited.engine.tag,
            visited.columns,
            tuple(new_relations),
            unique_keys=frozenset(),
            extra_doomed_by=visited.extra_doomed_by,
        ).assert_checked_and_simplified()

    def _fail(self, relation: Relation[_T]) -> Relation[_T]:
        raise EngineError(
            f"Cannot push join to {self.relation} through {relation} to satisfy engine consistency."
        )
