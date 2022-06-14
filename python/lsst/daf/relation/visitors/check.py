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

__all__ = ("Check",)

import itertools
from typing import TYPE_CHECKING

from .. import operations
from .._relation_visitor import RelationVisitor
from .._exceptions import (
    InvalidSliceError,
    MismatchedUnionError,
    MissingColumnError,
    UnmatchedJoinConditionError,
)

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._leaf_relation import LeafRelation


class Check(RelationVisitor[_T, None]):
    def visit_leaf(self, visited: LeafRelation[_T]) -> None:
        pass

    def visit_join(self, visited: operations.JoinRelation[_T]) -> None:
        for condition in visited.conditions:
            c0, c1 = condition.columns_required
            for r0, r1 in itertools.permutations(visited.relations, 2):
                if c0 <= r0.columns and c1 <= r1.columns:
                    break
            else:
                raise UnmatchedJoinConditionError(f"No match for join condition {condition}.")

    def visit_projected(self, visited: operations.ProjectedRelation[_T]) -> None:
        if not (visited.columns <= visited.base.columns):
            raise MissingColumnError(
                f"Cannot project column(s) {set(visited.columns) - visited.base.columns} "
                f"that are not present in the base relation {visited.base}."
            )

    def visit_selected(self, visited: operations.SelectedRelation[_T]) -> None:
        for p in visited.predicates:
            if not p.columns_required <= visited.base.columns:
                raise MissingColumnError(
                    f"Predicate {p} for base relation {visited.base} needs "
                    f"columns {p.columns_required - visited.base.columns}."
                )

    def visit_sliced(self, visited: operations.SlicedRelation[_T]) -> None:
        if not visited.order_by:
            raise InvalidSliceError("Cannot slice an unordered relation.")
        if not visited.offset and visited.limit is None:
            raise InvalidSliceError(
                "Cannot order a relation unless it is being sliced with nontrivial offset and/or limit."
            )

    def visit_union(self, visited: operations.UnionRelation[_T]) -> None:
        for relation in visited.relations:
            if relation.columns != visited.columns:
                raise MismatchedUnionError(
                    f"Mismatched union columns: {set(relation.columns)} != {set(visited.columns)} "
                    f"for relation {relation}."
                )
