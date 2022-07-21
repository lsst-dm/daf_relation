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
    "Expression",
    "ExpressionVisitor",
    "Literal",
    "Reference",
    "Function",
)

from abc import abstractmethod
import dataclasses
from typing import Any, Generic, TypeVar

from .._columns import _T

from .base import BaseExpression, BaseLiteral, BaseReference, BaseFunction
from ._predicate import Predicate, PredicateVisitor

_U = TypeVar("_U")


class Expression(BaseExpression[_T]):
    @abstractmethod
    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        ...

    def eq(self, other: Expression[_T]) -> Predicate[_T]:
        return PredicateFunction[_T]("__eq__", (self, other))

    def ne(self, other: Expression[_T]) -> Predicate[_T]:
        return PredicateFunction[_T]("__ne__", (self, other))

    def lt(self, other: Expression[_T]) -> Predicate[_T]:
        return PredicateFunction[_T]("__lt__", (self, other))

    def gt(self, other: Expression[_T]) -> Predicate[_T]:
        return PredicateFunction[_T]("__gt__", (self, other))

    def le(self, other: Expression[_T]) -> Predicate[_T]:
        return PredicateFunction[_T]("__le__", (self, other))

    def ge(self, other: Expression[_T]) -> Predicate[_T]:
        return PredicateFunction[_T]("__ge__", (self, other))


class ExpressionVisitor(Generic[_T, _U]):
    @abstractmethod
    def visit_literal(self, visited: Literal[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_reference(self, visited: Reference[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_function(self, visited: Function[_T]) -> _U:
        raise NotImplementedError()


@dataclasses.dataclass
class Literal(BaseLiteral[_T, Any], Expression[_T]):
    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_literal(self)


@dataclasses.dataclass
class Reference(BaseReference[_T], Expression[_T]):
    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_reference(self)


@dataclasses.dataclass
class Function(BaseFunction[_T, Expression[_T]]):
    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_function(self)


@dataclasses.dataclass
class PredicateFunction(BaseFunction[_T, Expression[_T]], Predicate[_T]):
    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_predicate_function(self)
