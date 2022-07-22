# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("ToSqlBooleans",)

import dataclasses
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Generic, TypeVar

import sqlalchemy

from .. import column_expressions
from .._columns import _T
from .._exceptions import EngineError
from ._to_logical_column import ToLogicalColumn

if TYPE_CHECKING:
    from ._engine import Engine

_L = TypeVar("_L")


@dataclasses.dataclass(eq=False, slots=True)
class ToSqlBooleans(
    column_expressions.PredicateVisitor[_T, Iterable[sqlalchemy.sql.ColumnElement]], Generic[_T, _L]
):
    def __init__(self, engine: Engine[_T, _L], columns_available: Mapping[_T, _L]):
        self.to_logical_column = ToLogicalColumn(engine, columns_available)

    to_logical_column: ToLogicalColumn[_T, _L]

    @property
    def engine(self) -> Engine:
        return self.to_logical_column.engine

    @property
    def columns_available(self) -> Mapping[_T, _L]:
        return self.to_logical_column.columns_available

    def visit_predicate_literal(
        self, visited: column_expressions.PredicateLiteral[_T]
    ) -> Iterable[sqlalchemy.sql.ColumnElement]:
        return (sqlalchemy.sql.literal(visited.value),)

    def visit_predicate_reference(
        self, visited: column_expressions.PredicateReference[_T]
    ) -> Iterable[sqlalchemy.sql.ColumnElement]:
        return (self.columns_available[visited.tag],)

    def visit_predicate_function(
        self, visited: column_expressions.PredicateFunction[_T]
    ) -> Iterable[sqlalchemy.sql.ColumnElement]:
        if (function := self.engine.get_column_function(visited.name)) is not None:
            return (function(*[arg.visit(self.to_logical_column) for arg in visited.args]),)
        raise EngineError(f"Predicate function {visited.name!r} is not supported by engine {self.engine}.")

    def visit_logical_not(
        self, visited: column_expressions.LogicalNot[_T]
    ) -> Iterable[sqlalchemy.sql.ColumnElement]:
        return (sqlalchemy.sql.not_(visited.base.visit(self)),)

    def visit_logical_and(
        self, visited: column_expressions.LogicalAnd[_T]
    ) -> Iterable[sqlalchemy.sql.ColumnElement]:
        return [operand.visit(self) for operand in visited.operands]

    def visit_logical_or(
        self, visited: column_expressions.LogicalOr[_T]
    ) -> Iterable[sqlalchemy.sql.ColumnElement]:
        return (sqlalchemy.sql.or_(*[operand.visit(self) for operand in visited.operands]),)
