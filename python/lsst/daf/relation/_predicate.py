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

__all__ = ("Predicate", "ConstantComparison", "ComparisonOperator")

import enum
import operator
from abc import abstractmethod
from collections.abc import Set, Mapping
from typing import TYPE_CHECKING, Any, Generic

import sqlalchemy

from ._columns import _T

if TYPE_CHECKING:
    from ._engines import EngineTag
    from ._serialization import DictWriter
    from . import iteration
    from . import sql


class Predicate(Generic[_T]):
    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def columns_required(self) -> Set[_T]:
        """The columns required to compute this expression
        (`~collections.abc.Set`).
        """
        raise NotImplementedError()

    @abstractmethod
    def supports_engine(self, engine: EngineTag) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def serialize(self, writer: DictWriter[_T]) -> dict[str, Any]:
        raise NotImplementedError()


class ComparisonOperator(enum.Enum):
    EQ = operator.eq
    LT = operator.lt
    LE = operator.le
    GT = operator.gt
    GE = operator.ge


class ConstantComparison(Predicate[_T]):
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
        return True

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
