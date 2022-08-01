# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the relations of the GNU General Public License as published by
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

__all__ = ("Union",)

from collections.abc import Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import immutable

from .._columns import _T, UniqueKey, compute_join_unique_keys, is_unique_key_covered
from .._engine import Engine
from .._exceptions import ColumnError
from .._relation import BinaryOperation, Relation

if TYPE_CHECKING:
    from .. import column_expressions
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Union(BinaryOperation[_T]):
    """An operation `.Relation` that combines the rows of its input relations.

    Parameters
    ----------
    TODO
    unique_keys : `~collections.abc.Set` [ `.UniqueKey` ]
        Set of sets that represent multi-column unique constraints that will be
        *naturally* satisfied by this union, even if the engine does not take
        any extra action to remove duplicates.  If not provided or empty, the
        returned relation does not guarantee uniqueness.
    """

    def __init__(
        self,
        lhs: Relation[_T],
        rhs: Relation[_T],
        unique_keys: Set[UniqueKey[_T]] = frozenset(),
    ):
        assert (
            lhs.engine is None or rhs.engine is None or lhs.engine == rhs.engine
        ), "should be guaranteed by calling factory"
        if lhs.columns != rhs.columns:
            raise ColumnError(f"Mismatched union columns: {set(lhs.columns)} != {set(rhs.columns)}.")
        for key in unique_keys:
            if not is_unique_key_covered(key, lhs.unique_keys):
                raise ColumnError(
                    f"Union is declared to have unique key {set(key)}, but "
                    f"first operand {lhs} is not unique with those columns."
                )
            if not is_unique_key_covered(key, rhs.unique_keys):
                raise ColumnError(
                    f"Union is declared to have unique key {set(key)}, but "
                    f"second operand {rhs} is not unique with those columns."
                )
        self._lhs = lhs
        self._rhs = rhs
        self._unique_keys = unique_keys

    @property
    def lhs(self) -> Relation[_T]:
        # Docstring inherited.
        return self._lhs

    @property
    def rhs(self) -> Relation[_T]:
        # Docstring inherited.
        return self._rhs

    def __str__(self) -> str:
        return f"({self.lhs} ∪ {self.rhs})"

    @property
    def engine(self) -> Engine[_T]:
        # Docstring inherited.
        return self.lhs.engine if self.lhs.engine is not None else self.rhs.engine

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self.lhs.columns

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return self._unique_keys

    def rebase(self, lhs: Relation[_T], rhs: Relation[_T]) -> Relation[_T]:
        # Docstring inherited.
        return lhs.union(rhs, self._unique_keys)

    def _try_join(
        self, rhs: Relation[_T], condition: column_expressions.JoinCondition[_T]
    ) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_join(rhs, condition)) is not None:
            return result
        if (new_first := self.lhs._try_join(rhs, condition)) is None:
            return None
        if (new_second := self.rhs._try_join(rhs, condition)) is None:
            return None
        return Union(new_first, new_second, compute_join_unique_keys(self.unique_keys, rhs.unique_keys))

    def _try_selection(self, predicate: column_expressions.Predicate[_T]) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_selection(predicate)) is not None:
            return result
        if (new_first := self.lhs._try_selection(predicate)) is None:
            return None
        if (new_second := self.rhs._try_selection(predicate)) is None:
            return None
        return Union(new_first, new_second, self.unique_keys)

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_union(self)
