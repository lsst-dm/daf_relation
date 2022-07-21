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
from collections.abc import Sequence, Set
from typing import TYPE_CHECKING, Generic, TypeVar

from lsst.utils.classes import Singleton, cached_getter

from .._columns import _T

if TYPE_CHECKING:
    from ._expression import ColumnExpression


_U = TypeVar("_U")


class Predicate(Generic[_T]):
    __slots__ = ()

    @property
    @abstractmethod
    def columns_required(self) -> Set[_T]:
        raise NotImplementedError()

    def logical_not(self: Predicate[_T]) -> Predicate[_T]:
        return LogicalNot(self)

    def logical_and(*operands: Predicate[_T]) -> Predicate[_T]:
        return LogicalAnd(operands)

    def logical_or(*operands: Predicate[_T]) -> Predicate[_T]:
        return LogicalOr(operands)

    @abstractmethod
    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        raise NotImplementedError()


class PredicateVisitor(Generic[_T, _U]):
    def visit_true_literal(self, visited: TrueLiteral[_T]) -> _U:
        raise NotImplementedError()

    def visit_false_literal(self, visited: FalseLiteral[_T]) -> _U:
        raise NotImplementedError()

    def visit_boolean_column_reference(self, visited: BooleanColumnReference[_T]) -> _U:
        raise NotImplementedError()

    def visit_unary_predicate(self, visited: UnaryPredicate[_T]) -> _U:
        raise NotImplementedError()

    def visit_binary_predicate(self, visited: BinaryPredicate[_T]) -> _U:
        raise NotImplementedError()

    def visit_logical_not(self, visited: LogicalNot[_T]) -> _U:
        raise NotImplementedError()

    def visit_logical_and(self, visited: LogicalAnd[_T]) -> _U:
        raise NotImplementedError()

    def visit_logical_or(self, visited: LogicalOr[_T]) -> _U:
        raise NotImplementedError()


class TrueLiteral(Predicate[_T], metaclass=Singleton):
    __slots__ = ()

    @property
    def columns_required(self) -> Set[_T]:
        return frozenset()

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_true_literal(self)


class FalseLiteral(Predicate[_T], metaclass=Singleton):
    __slots__ = ()

    @property
    def columns_required(self) -> Set[_T]:
        return frozenset()

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_false_literal(self)


@dataclasses.dataclass(slots=True)
class BooleanColumnReference(Predicate[_T]):

    tag: _T

    @property
    def columns_required(self) -> Set[_T]:
        return {self.tag}

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_boolean_column_reference(self)


@dataclasses.dataclass(slots=True)
class UnaryPredicate(Predicate[_T]):

    base: ColumnExpression[_T]
    name: str

    @property
    def columns_required(self) -> Set[_T]:
        return self.base.columns_required

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_unary_predicate(self)


@dataclasses.dataclass(slots=True)
class BinaryPredicate(Predicate[_T]):

    lhs: ColumnExpression[_T]
    rhs: ColumnExpression[_T]
    name: str

    @property
    def columns_required(self) -> Set[_T]:
        return self.lhs.columns_required | self.rhs.columns_required

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_binary_predicate(self)


@dataclasses.dataclass
class LogicalNot(Predicate[_T]):
    base: Predicate[_T]

    @property
    def columns_required(self) -> Set[_T]:
        return self.base.columns_required

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_logical_not(self)


@dataclasses.dataclass(slots=True)
class LogicalAnd(Predicate[_T]):

    operands: Sequence[Predicate[_T]]

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> Set[_T]:
        result: set[_T] = set()
        for operand in self.operands:
            result.update(operand.columns_required)
        return result

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_logical_and(self)


@dataclasses.dataclass(slots=True)
class LogicalOr(Predicate[_T]):

    operands: Sequence[Predicate[_T]]

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> Set[_T]:
        result: set[_T] = set()
        for operand in self.operands:
            result.update(operand.columns_required)
        return result

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_logical_or(self)
