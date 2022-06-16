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
    EngineMismatchError,
    InconsistentUniqueKeysError,
    InvalidSliceError,
    MismatchedUnionError,
    MissingColumnError,
    UnmatchedJoinConditionError,
)

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._leaf import Leaf
    from .._relation import Relation


class Check(RelationVisitor[_T, None]):
    def __init__(self, check_engine_consistency: bool):
        self.check_engine_consistency = check_engine_consistency

    def visit_leaf(self, visited: Leaf[_T]) -> None:
        self._check_unique_keys(visited)

    def visit_join(self, visited: operations.Join[_T]) -> None:
        for relation in visited.relations:
            if self.check_engine_consistency and relation.engine != visited.engine:
                raise EngineMismatchError(
                    f"Join member {relation} has engine {relation.engine}, "
                    f"while join has {visited.engine}."
                )
        for condition in visited.conditions:
            if self.check_engine_consistency and visited.engine not in condition.state:
                raise EngineMismatchError(
                    f"Join condition {condition} supports engine(s) {set(condition.state.keys())}, "
                    f"while join has {visited.engine}."
                )
            if not condition.match(visited.relations):
                raise UnmatchedJoinConditionError(f"No match for join condition {condition}.")

    def visit_projection(self, visited: operations.Projection[_T]) -> None:
        if not (visited.columns <= visited.base.columns):
            raise MissingColumnError(
                f"Cannot project column(s) {set(visited.columns) - visited.base.columns} "
                f"that are not present in the base relation {visited.base}."
            )

    def visit_selection(self, visited: operations.Selection[_T]) -> None:
        for p in visited.predicates:
            if self.check_engine_consistency and visited.engine not in p.state:
                raise EngineMismatchError(
                    f"Predicate {p} supports engine(s) {set(p.state.keys())}, "
                    f"while relation has {visited.engine}."
                )
            if not p.columns_required <= visited.base.columns:
                raise MissingColumnError(
                    f"Predicate {p} for base relation {visited.base} needs "
                    f"columns {p.columns_required - visited.base.columns}."
                )

    def visit_slice(self, visited: operations.Slice[_T]) -> None:
        if not visited.order_by:
            raise InvalidSliceError("Cannot slice an unordered relation.")
        if not visited.offset and visited.limit is None:
            raise InvalidSliceError(
                "Cannot order a relation unless it is being sliced with nontrivial offset and/or limit."
            )
        for o in visited.order_by:
            if self.check_engine_consistency and visited.engine not in o.state:
                raise EngineMismatchError(
                    f"Order-by term {o} supports engine(s) {set(o.state.keys())}, "
                    f"while relation has {visited.engine}."
                )
            if not o.columns_required <= visited.base.columns:
                raise MissingColumnError(
                    f"Order-by term {o} for base relation {visited.base} needs "
                    f"columns {o.columns_required - visited.base.columns}."
                )

    def visit_transfer(self, visited: operations.Transfer[_T]) -> None:
        pass

    def visit_union(self, visited: operations.Union[_T]) -> None:
        self._check_unique_keys(visited)
        for relation in visited.relations:
            for key in visited.unique_keys:
                if key not in relation.unique_keys and not any(
                    key.issuperset(relation_key) for relation_key in relation.unique_keys
                ):
                    raise InconsistentUniqueKeysError(
                        f"Union is declared to have unique key {set(key)}, but "
                        f"member {relation} is not unique with those columns."
                    )
            if self.check_engine_consistency and relation.engine != visited.engine:
                raise EngineMismatchError(
                    f"Union member {relation} has engine {relation.engine}, "
                    f"while union has {visited.engine}."
                )
            if relation.columns != visited.columns:
                raise MismatchedUnionError(
                    f"Mismatched union columns: {set(relation.columns)} != {set(visited.columns)} "
                    f"for relation {relation}."
                )

    def _check_unique_keys(self, relation: Relation[_T]) -> None:
        for k1, k2 in itertools.permutations(relation.unique_keys, 2):
            if not k1.issuperset(k2):
                raise InconsistentUniqueKeysError(
                    f"Relation {relation} unique key {set(k1)} is redundant, "
                    f"since {set(k2)} is already unique."
                )
