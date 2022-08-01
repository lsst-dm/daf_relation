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
    "LogicalNot",
    "LogicalAnd",
    "LogicalOr",
    "Predicate",
    "PredicateVisitor",
    "PredicateLiteral",
    "PredicateReference",
)

import dataclasses
from abc import abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from lsst.utils.classes import cached_getter

from .._columns import _T
from .._engine import Engine

if TYPE_CHECKING:
    from ._container import InContainer
    from ._expression import PredicateFunction

_U = TypeVar("_U")


class Predicate(Generic[_T]):
    @property
    @abstractmethod
    def columns_required(self) -> Set[_T]:
        raise NotImplementedError()

    dtype: ClassVar[type[bool]]

    @abstractmethod
    def is_supported_by(self, engine: Engine[_T]) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        raise NotImplementedError()

    def logical_not(self) -> Predicate[_T]:
        return LogicalNot(self)

    def logical_and(*operands: Predicate[_T]) -> Predicate[_T]:
        all_operands: list[Predicate[_T]] = []
        for operand in operands:
            all_operands.extend(operand.flatten_logical_and())
        return LogicalAnd(tuple(all_operands))

    def logical_or(*operands: Predicate[_T]) -> Predicate[_T]:
        all_operands: list[Predicate[_T]] = []
        for operand in operands:
            all_operands.extend(operand.flatten_logical_or())
        return LogicalOr(tuple(all_operands))

    def flatten_logical_and(self) -> tuple[Predicate[_T], ...]:
        return (self,)

    def flatten_logical_or(self) -> tuple[Predicate[_T], ...]:
        return (self,)


class PredicateVisitor(Generic[_T, _U]):
    @abstractmethod
    def visit_predicate_literal(self, visited: PredicateLiteral[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_predicate_reference(self, visited: PredicateReference[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_predicate_function(self, visited: PredicateFunction[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_in_container(self, visited: InContainer[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_logical_not(self, visited: LogicalNot[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_logical_and(self, visited: LogicalAnd[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_logical_or(self, visited: LogicalOr[_T]) -> _U:
        raise NotImplementedError()


@dataclasses.dataclass
class PredicateLiteral(Predicate[_T]):

    value: bool

    @property
    def columns_required(self) -> Set[_T]:
        return frozenset()

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return True

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_predicate_literal(self)


@dataclasses.dataclass
class PredicateReference(Predicate[_T]):

    tag: _T

    @property
    def columns_required(self) -> Set[_T]:
        return frozenset()

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return True

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_predicate_reference(self)


@dataclasses.dataclass
class LogicalNot(Predicate[_T]):
    base: Predicate[_T]

    @property
    def columns_required(self) -> Set[_T]:
        return self.base.columns_required

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return self.base.is_supported_by(engine)

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_logical_not(self)


@dataclasses.dataclass
class LogicalAnd(Predicate[_T]):

    operands: tuple[Predicate[_T], ...]

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> Set[_T]:
        result: set[_T] = set()
        for operand in self.operands:
            result.update(operand.columns_required)
        return result

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return all(operand.is_supported_by(engine) for operand in self.operands)

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_logical_and(self)

    def flatten_logical_and(self) -> tuple[Predicate[_T], ...]:
        return self.operands


@dataclasses.dataclass
class LogicalOr(Predicate[_T]):

    operands: tuple[Predicate[_T], ...]

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> Set[_T]:
        result: set[_T] = set()
        for operand in self.operands:
            result.update(operand.columns_required)
        return result

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return all(operand.is_supported_by(engine) for operand in self.operands)

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_logical_or(self)

    def flatten_logical_or(self) -> tuple[Predicate[_T], ...]:
        return self.operands
