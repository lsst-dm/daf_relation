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

__all__ = ()

import dataclasses
from abc import abstractmethod
from collections.abc import Set
from typing import Any, Generic, TypeVar

from lsst.utils.sets.unboundable import FrozenUnboundableSet, UnboundableSet

from .._columns import _T
from .._engine import Engine

_U = TypeVar("_U")


class ColumnExpression(Generic[_T]):

    __slots__ = ()

    @property
    @abstractmethod
    def columns_required(self) -> Set[_T]:
        raise NotImplementedError()

    @abstractmethod
    def is_supported_by(self, engine: Engine) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def visit(self, visitor: ColumnExpressionVisitor[_T, _U]) -> _U:
        raise NotImplementedError()


class ColumnExpressionVisitor(Generic[_T, _U]):
    def visit_column_literal(self, visited: ColumnLiteral[_T]) -> _U:
        raise NotImplementedError()

    def visit_column_reference(self, visited: ColumnReference[_T]) -> _U:
        raise NotImplementedError()

    def visit_unary_column_expression(self, visited: UnaryColumnExpression[_T]) -> _U:
        raise NotImplementedError()

    def visit_binary_column_expression(self, visited: BinaryColumnExpression[_T]) -> _U:
        raise NotImplementedError()


@dataclasses.dataclass
class ColumnLiteral(ColumnExpression[_T]):
    value: Any
    engines_supported: UnboundableSet[Engine] = FrozenUnboundableSet.full

    @property
    def columns_required(self) -> Set[_T]:
        return frozenset()

    def is_supported_by(self, engine: Engine) -> bool:
        return engine in self.engines_supported

    def visit(self, visitor: ColumnExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_column_literal(self)


class ColumnReference(ColumnExpression[_T]):
    def __init__(self, tag: _T):
        self.tag = tag

    @property
    def columns_required(self) -> Set[_T]:
        return {self.tag}

    def is_supported_by(self, engine: Engine) -> bool:
        return True

    def visit(self, visitor: ColumnExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_column_reference(self)


class UnaryColumnExpression(ColumnExpression[_T]):

    name: str
    base: ColumnExpression[_T]

    @property
    def columns_required(self) -> Set[_T]:
        return self.base.columns_required

    def is_supported_by(self, engine: Engine) -> bool:
        return engine.get_unary_function(self.name) is not None and self.base.is_supported_by(engine)

    def visit(self, visitor: ColumnExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_unary_column_expression(self)


class BinaryColumnExpression(ColumnExpression[_T]):

    name: str
    lhs: ColumnExpression[_T]
    rhs: ColumnExpression[_T]

    @property
    def columns_required(self) -> Set[_T]:
        return self.lhs.columns_required | self.rhs.columns_required

    def is_supported_by(self, engine: Engine) -> bool:
        return (
            engine.get_binary_function(self.name) is not None
            and self.lhs.is_supported_by(engine)
            and self.rhs.is_supported_by(engine)
        )

    def visit(self, visitor: ColumnExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_binary_column_expression(self)
