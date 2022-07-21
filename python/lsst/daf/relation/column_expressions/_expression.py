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

__all__ = (
    "ColumnExpression",
    "ColumnExpressionVisitor",
    "ColumnLiteral",
    "ColumnReference",
    "ColumnFunction",
)

from abc import abstractmethod
import dataclasses
from typing import Any, Generic, TypeVar

from .._columns import _T

from .base import BaseColumnExpression, BaseColumnLiteral, BaseColumnReference, BaseColumnFunction
from ._predicate import Predicate, PredicateVisitor

_U = TypeVar("_U")


class ColumnExpression(BaseColumnExpression[_T]):
    @abstractmethod
    def visit(self, visitor: ColumnExpressionVisitor[_T, _U]) -> _U:
        ...

    def eq(self, other: ColumnExpression[_T]) -> Predicate[_T]:
        return BooleanColumnFunction[_T]("__eq__", (self, other))

    def ne(self, other: ColumnExpression[_T]) -> Predicate[_T]:
        return BooleanColumnFunction[_T]("__ne__", (self, other))

    def lt(self, other: ColumnExpression[_T]) -> Predicate[_T]:
        return BooleanColumnFunction[_T]("__lt__", (self, other))

    def gt(self, other: ColumnExpression[_T]) -> Predicate[_T]:
        return BooleanColumnFunction[_T]("__gt__", (self, other))

    def le(self, other: ColumnExpression[_T]) -> Predicate[_T]:
        return BooleanColumnFunction[_T]("__le__", (self, other))

    def ge(self, other: ColumnExpression[_T]) -> Predicate[_T]:
        return BooleanColumnFunction[_T]("__ge__", (self, other))


class ColumnExpressionVisitor(Generic[_T, _U]):
    @abstractmethod
    def visit_column_literal(self, visited: ColumnLiteral[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_column_reference(self, visited: ColumnReference[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_column_function(self, visited: ColumnFunction[_T]) -> _U:
        raise NotImplementedError()


@dataclasses.dataclass
class ColumnLiteral(BaseColumnLiteral[_T, Any], ColumnExpression[_T]):
    def visit(self, visitor: ColumnExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_column_literal(self)


@dataclasses.dataclass
class ColumnReference(BaseColumnReference[_T], ColumnExpression[_T]):
    def visit(self, visitor: ColumnExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_column_reference(self)


@dataclasses.dataclass
class ColumnFunction(BaseColumnFunction[_T, ColumnExpression[_T]]):
    def visit(self, visitor: ColumnExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_column_function(self)


@dataclasses.dataclass
class BooleanColumnFunction(BaseColumnFunction[_T, ColumnExpression[_T]], Predicate[_T]):
    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_boolean_column_function(self)
