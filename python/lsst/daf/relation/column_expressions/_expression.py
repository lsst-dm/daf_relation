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
    "PredicateFunction",
)

import dataclasses
from abc import abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from lsst.utils.sets.ellipsis import EllipsisType
from lsst.utils.sets.unboundable import FrozenUnboundableSet, UnboundableSet

from .._columns import _T
from ._predicate import Predicate, PredicateVisitor
from .base import BaseExpression, BaseFunction, BaseLiteral, BaseReference

if TYPE_CHECKING:
    from .._engine import Engine

_U = TypeVar("_U")


class Expression(BaseExpression[_T]):
    @abstractmethod
    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        ...

    @classmethod
    def reference(cls, tag: _T) -> Expression[_T]:
        return Reference(tag)

    @classmethod
    def literal(cls, value: Any) -> Expression[_T]:
        return Literal(value)

    @classmethod
    def function(
        cls,
        name: str,
        *args: Expression[_T],
        supporting_engines: Set[Engine[_T] | type[Engine[_T]]]
        | EllipsisType
        | UnboundableSet[Engine[_T] | type[Engine[_T]]] = FrozenUnboundableSet.full,
    ) -> Expression[_T]:
        return Function(name, args, supporting_engines=FrozenUnboundableSet.coerce(supporting_engines))

    def eq(self, other: Expression[_T]) -> Predicate[_T]:
        return self.predicate_function("__eq__", other)

    def ne(self, other: Expression[_T]) -> Predicate[_T]:
        return self.predicate_function("__ne__", other)

    def lt(self, other: Expression[_T]) -> Predicate[_T]:
        return self.predicate_function("__lt__", other)

    def gt(self, other: Expression[_T]) -> Predicate[_T]:
        return self.predicate_function("__gt__", other)

    def le(self, other: Expression[_T]) -> Predicate[_T]:
        return self.predicate_function("__le__", other)

    def ge(self, other: Expression[_T]) -> Predicate[_T]:
        return self.predicate_function("__ge__", other)

    def predicate_function(
        self,
        name: str,
        *args: Expression[_T],
        supporting_engines: Set[Engine[_T] | type[Engine[_T]]]
        | EllipsisType
        | UnboundableSet[Engine[_T] | type[Engine[_T]]] = FrozenUnboundableSet.full,
    ) -> Predicate[_T]:
        return PredicateFunction[_T](
            name,
            (self,) + args,
        )


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
class Function(BaseFunction[_T, Expression[_T]], Expression[_T]):
    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_function(self)


@dataclasses.dataclass
class PredicateFunction(BaseFunction[_T, Expression[_T]], Predicate[_T]):
    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_predicate_function(self)
