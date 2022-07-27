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

__all__ = ("Selection",)

from collections.abc import Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import immutable

from .._columns import _T, UniqueKey
from .._exceptions import ColumnError, EngineError
from .._relation import Relation, UnaryOperation

if TYPE_CHECKING:
    from .. import column_expressions
    from .._engine import Engine
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Selection(UnaryOperation[_T]):
    """An operation `.Relation` that filters rows via `.Predicate` objects.

    Parameters
    ----------
    base : `.Relation`
        Relation this operation acts upon.
    predicate : `.Predicate`
        Predicate to apply.
    """

    def __init__(self, base: Relation[_T], predicate: column_expressions.Predicate[_T]):
        if not predicate.columns_required <= base.columns:
            raise ColumnError(
                f"Predicate {predicate} for base relation {base} needs "
                f"columns {predicate.columns_required - base.columns}."
            )
        if not predicate.is_supported_by(base.engine):
            raise EngineError(f"Predicate {predicate} does not support engine {base.engine}.")
        self._base = base
        self.predicate = predicate

    @property
    def base(self) -> Relation[_T]:
        # Docstring inherited.
        return self._base

    predicate: column_expressions.Predicate[_T]
    """Predicate to apply (`.Predicate`)."""

    def __str__(self) -> str:
        return f"σ({self.base!s}, {self.predicate})"

    @property
    def engine(self) -> Engine[_T]:
        # Docstring inherited.
        return self.base.engine

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
        return base.selection(self.predicate)

    def _try_join(
        self, rhs: Relation[_T], condition: column_expressions.JoinCondition[_T]
    ) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_join(rhs, condition)) is not None:
            return result
        if (new_base := self.base._try_join(rhs, condition)) is not None:
            return Selection(new_base, self.predicate)
        return None

    def _try_selection(self, predicate: column_expressions.Predicate[_T]) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_selection(predicate)) is not None:
            return result
        if (new_base := self.base._try_selection(predicate)) is not None:
            return Selection(new_base, self.predicate)
        return None

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_selection(self)
