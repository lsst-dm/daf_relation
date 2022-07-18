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

__all__ = ("Join",)

from collections.abc import Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import cached_getter, immutable

from .._columns import _T, UniqueKey, compute_join_unique_keys
from .._engines import EngineTag
from .._exceptions import ColumnError, EngineError
from .._relation import Relation

if TYPE_CHECKING:
    from .._join_condition import JoinCondition
    from .._predicate import Predicate
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Join(Relation[_T]):
    """An operation `.Relation` that performs a natural join.

    A natural join includes all columns from all input relations while keeping
    only rows where all input relations have the same values for any common
    columns.

    Parameters
    ----------
    lhs : `.Relation`
        Left-hand side operand.
    rhs : `.Relation`
        Right-hand side operand.
    condition : `.JoinCondition`, optional
        A custom (generally non-equality) condition for the join, to be applied
        in addition to common-column equality conditions.
    """

    def __init__(
        self,
        lhs: Relation[_T],
        rhs: Relation[_T],
        condition: JoinCondition[_T] | None = None,
    ):
        if lhs.engine != rhs.engine:
            raise EngineError(f"Inconsistent engines for join: {lhs.engine} != {rhs.engine}.")
        if condition is not None:
            if not condition.matches(lhs.columns, rhs.columns):
                raise ColumnError(
                    f"Join condition {condition} does not match relation columns "
                    f"{set(lhs.columns)}, {set(rhs.columns)}"
                )
            if not condition.supports_engine(rhs.engine):
                raise EngineError(f"Join engine {rhs.engine} is not supported by condition {condition}.")
        self.lhs = lhs
        self.rhs = rhs
        self.condition = condition

    lhs: Relation[_T]
    """Left-hand side operand (`.Relation`).
    """

    rhs: Relation[_T]
    """Right-hand side operand (`.Relation`).
    """

    condition: JoinCondition[_T] | None
    """A custom (generally non-equality) condition for the join, to be applied
    in addition to common-column equality conditions (`.JoinCondition` or
    `None`).
    """

    def __str__(self) -> str:
        return f"({self.lhs} ⋈ {self.rhs})"

    @property
    def engine(self) -> EngineTag:
        # Docstring inherited.
        return self.lhs.engine

    @property  # type: ignore
    @cached_getter
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self.lhs.columns | self.rhs.columns

    @property  # type: ignore
    @cached_getter
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return compute_join_unique_keys(self.lhs.unique_keys, self.rhs.unique_keys)

    def _try_join(self, rhs: Relation[_T], condition: JoinCondition[_T] | None) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_join(rhs, condition)) is not None:
            return result

        def try_branch_join(branch: Relation[_T]) -> Relation[_T] | None:
            if not branch.columns >= self.columns & rhs.columns:
                # Inserting new join at this existing join branch changes the
                # automatic part of the join condition.
                return None
            if condition is not None and not condition.columns_required[0] <= branch.columns:
                # Explicit join condition doesn't work on this branch, either.
                return None
            return branch._try_join(rhs, condition)

        if (new_lhs := try_branch_join(self.lhs)) is not None:
            return Join(new_lhs, self.rhs, self.condition)
        if (new_rhs := try_branch_join(self.rhs)) is not None:
            return Join(self.lhs, new_rhs, self.condition)
        return None

    def _try_selection(self, predicate: Predicate[_T]) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_selection(predicate)) is not None:
            return result

        def try_branch_selection(branch: Relation[_T]) -> Relation[_T]:
            if not branch.columns >= predicate.columns_required:
                return branch
            return new_branch if (new_branch := branch._try_selection(predicate)) is not None else branch

        new_lhs = try_branch_selection(self.lhs)
        new_rhs = try_branch_selection(self.rhs)
        # If we were able to apply the selection to either branch, the join
        # will effectively apply it to the other.  If we can apply it to both,
        # we do, because you almost always want to apply selections as early
        # and often as possible to evaluate a relation expression efficiently.
        if new_lhs is not self.lhs or new_rhs is not self.rhs:
            return Join(new_lhs, new_rhs, self.condition)
        return None

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_join(self)
