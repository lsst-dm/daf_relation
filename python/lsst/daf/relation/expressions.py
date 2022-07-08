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

__all__ = (
    "ConstantComparisonPredicate",
    "ComparisonOperator",
    "SingleColumnOrderByTerm",
    "DescendingOrderByTerm",
)

import enum
import operator
from collections.abc import Mapping, Set
from typing import TYPE_CHECKING, Any, cast

import sqlalchemy

from . import iteration, sql
from ._columns import _T
from ._order_by_term import OrderByTerm
from ._predicate import Predicate

if TYPE_CHECKING:
    from ._engines import EngineTag
    from ._serialization import DictWriter


class ComparisonOperator(enum.Enum):
    EQ = operator.eq
    LT = operator.lt
    LE = operator.le
    GT = operator.gt
    GE = operator.ge


class ConstantComparisonPredicate(Predicate[_T]):
    def __init__(
        self,
        column: _T,
        value: Any,
        comparison_operator: ComparisonOperator = ComparisonOperator.EQ,
    ):
        self.column = column
        self.value = value
        self.comparison_operator = comparison_operator

    def __eq__(self, other: Any) -> bool:
        if self.__class__ != other.__class__:
            return NotImplemented
        return (
            self.column == other.column
            and self.value == other.value
            and self.comparison_operator == other.comparison_operator
        )

    def __hash__(self) -> int:
        return hash((self.column, self.comparison_operator))

    @property
    def columns_required(self) -> Set[_T]:
        return {self.column}

    def supports_engine(self, engine: EngineTag) -> bool:
        return engine is iteration.engine or isinstance(engine, sql.Engine)

    def serialize(self, writer: DictWriter[_T]) -> dict[str, Any]:
        return {
            "type": "constant_comparison",
            "column": writer.write_column(self.column),
            "value": self.value,
        }

    def test_iteration_row(self, row: iteration.typing.Row[_T]) -> bool:
        return self.comparison_operator.value(row[self.column], self.value)

    def to_sql_boolean(
        self, logical_columns: Mapping[_T, Any], column_types: sql.ColumnTypeInfo[_T, Any]
    ) -> sqlalchemy.sql.ColumnElement:
        return self.comparison_operator.value(logical_columns[self.column], self.value)


class SingleColumnOrderByTerm(OrderByTerm[_T]):
    def __init__(self, column: _T):
        self.column = column

    @property
    def columns_required(self) -> Set[_T]:
        return {self.column}

    def supports_engine(self, engine: EngineTag) -> bool:
        return engine is iteration.engine or isinstance(engine, sql.Engine)

    def serialize(self, writer: DictWriter[_T]) -> dict[str, Any]:
        return {
            "type": "single_column",
            "column": writer.write_column(self.column),
        }

    def get_iteration_row_sort_key(self, row: iteration.typing.Row[_T]) -> iteration.typing.Sortable:
        return row[self.column]

    def get_iteration_row_sort_reverse(self) -> bool:
        return False

    def to_sql_sort_column(
        self, logical_columns: Mapping[_T, Any], column_types: sql.ColumnTypeInfo[_T, Any]
    ) -> sqlalchemy.sql.ColumnElement:
        return logical_columns[self.column]


class DescendingOrderByTerm(OrderByTerm[_T]):
    def __init__(self, base: OrderByTerm[_T]):
        self.base = base

    @property
    def columns_required(self) -> Set[_T]:
        return self.base.columns_required

    def supports_engine(self, engine: EngineTag) -> bool:
        return self.base.supports_engine(engine) and (
            engine is iteration.engine or isinstance(engine, sql.Engine)
        )

    def serialize(self, writer: DictWriter[_T]) -> dict[str, Any]:
        return {
            "type": "descending",
            "base": self.base.serialize(writer),
        }

    def get_iteration_row_sort_key(self, row: iteration.typing.Row[_T]) -> iteration.typing.Sortable:
        return cast(iteration.OrderByTermInterface, self.base).get_iteration_row_sort_key(row)

    def get_iteration_row_sort_reverse(self) -> bool:
        return not cast(iteration.OrderByTermInterface, self.base).get_iteration_row_sort_reverse()

    def to_sql_sort_column(
        self, logical_columns: Mapping[_T, Any], column_types: sql.ColumnTypeInfo[_T, Any]
    ) -> sqlalchemy.sql.ColumnElement:
        return (
            cast(sql.OrderByTermInterface, self.base).to_sql_sort_column(logical_columns, column_types).desc()
        )
