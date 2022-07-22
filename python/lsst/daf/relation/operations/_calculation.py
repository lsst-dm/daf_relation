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

__all__ = ("Calculation",)

from collections.abc import Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import cached_getter, immutable

from .._columns import _T, UniqueKey
from .._exceptions import ColumnError, EngineError
from .._relation import Relation

if TYPE_CHECKING:
    from .. import column_expressions
    from .._engine import Engine
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Calculation(Relation[_T]):
    """An operation `.Relation` that adds a new column whose values are
    calculated from one or more existing columns.

    Parameters
    ----------
    base : `.Relation`
        Relation this operation acts upon.
    columns : `frozenset` [ `.ColumnTag` ]
        Columns to propagate.
    """

    def __init__(self, base: Relation[_T], tag: _T, expression: column_expressions.Expression[_T]):
        if not (expression.columns_required <= base.columns):
            raise ColumnError(
                f"Cannot calculate column {tag} because expression requires "
                f"columns {set(expression.columns_required) - base.columns} "
                f"that are not present in the base relation {base}."
            )
        if not expression.is_supported_by(base.engine):
            raise EngineError(f"Column expression {expression} does not support engine {base.engine}.")
        self.base = base
        self.expression = expression
        self.tag = tag

    base: Relation[_T]
    """Relation this operation acts upon (`.Relation`).
    """

    def __str__(self) -> str:
        return f"({self.base!s} + {self.tag}={self.expression!s})"

    @property
    def engine(self) -> Engine[_T]:
        return self.base.engine

    @property  # type: ignore
    @cached_getter
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        result = set(self.base.columns)
        result.add(self.tag)
        return result

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return self.base.unique_keys

    def _try_join(
        self, rhs: Relation[_T], condition: column_expressions.JoinCondition[_T]
    ) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_join(rhs, condition)) is not None:
            return result
        if condition.lhs_columns <= self.base.columns:
            if (new_base := self.base._try_join(rhs, condition)) is not None:
                return Calculation(new_base, self.tag, self.expression)
        return None

    def _try_selection(self, predicate: column_expressions.Predicate[_T]) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_selection(predicate)) is not None:
            return result
        if predicate.columns_required <= self.base.columns:
            if (new_base := self.base._try_selection(predicate)) is not None:
                return Calculation(new_base, self.tag, self.expression)
        return None

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_calculation(self)
