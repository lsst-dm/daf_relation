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

__all__ = ("Transfer",)

from collections.abc import Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import cached_getter, immutable

from .._columns import _T, UniqueKey
from .._engine import Engine
from .._relation import Relation, UnaryOperation

if TYPE_CHECKING:
    from .. import column_expressions
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Transfer(UnaryOperation[_T]):
    """An operation `.Relation` that indicates a transfer from one engine to
    another.

    Parameters
    ----------
    base : `.Relation`
        Relation this operation acts upon, in the source engine.
    destination : `.EngineTag`
        Engine to transfer the relation to.

    Notes
    -----
    Like other operations, `Transfer` objects should only be constructed
    directly by code that can easily guarantee their `checked_and_simplified`
    invariants; in all other contexts, the `.Relation.transfer` factory should
    be used instead.

    See `.Relation.transfer` for the `checked_and_simplified` behavior for this
    class.
    """

    def __init__(self, base: Relation[_T], destination: Engine[_T]):
        self._base = base
        self._destination = destination

    @property
    def base(self) -> Relation[_T]:
        # Docstring inherited.
        return self._base

    def __str__(self) -> str:
        return f"{self.base!s} → {self._destination!s}"

    @property  # type: ignore
    @cached_getter
    def engine(self) -> Engine[_T]:
        # Docstring inherited.
        return self._destination

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self.base.columns

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return self.base.unique_keys

    def _rebase(self, base: Relation[_T]) -> Relation[_T]:
        # Docstring inherited.
        return base.transfer(self._destination)

    def _try_join(
        self, rhs: Relation[_T], condition: column_expressions.JoinCondition[_T]
    ) -> Relation[_T] | None:
        if (result := super()._try_join(rhs, condition)) is not None:
            return result
        if (new_base := self.base._try_join(rhs, condition)) is not None:
            return Transfer(new_base, self._destination)
        return None

    def _try_selection(self, predicate: column_expressions.Predicate[_T]) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_selection(predicate)) is not None:
            return result
        if (new_base := self.base._try_selection(predicate)) is not None:
            return Transfer(new_base, self._destination)
        return None

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_transfer(self)
