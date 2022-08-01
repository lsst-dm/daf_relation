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
    "Container",
    "RangeLiteral",
    "ExpressionSequence",
    "ContainerVisitor",
    "InContainer",
)

import dataclasses
from abc import abstractmethod
from collections.abc import Sequence, Set
from typing import TYPE_CHECKING, Generic, TypeVar

from lsst.utils.classes import cached_getter

from .._columns import _T
from .._engine import Engine
from .._exceptions import RelationalAlgebraError
from ._predicate import Predicate, PredicateVisitor

if TYPE_CHECKING:
    from ._expression import Expression

_U = TypeVar("_U")


class Container(Generic[_T]):
    @property
    @abstractmethod
    def columns_required(self) -> Set[_T]:
        raise NotImplementedError()

    @abstractmethod
    def is_supported_by(self, engine: Engine[_T]) -> bool:
        raise NotImplementedError()

    @property
    @abstractmethod
    def dtype(self) -> type | None:
        raise NotImplementedError()

    @abstractmethod
    def visit(self, visitor: ContainerVisitor[_T, _U]) -> _U:
        raise NotImplementedError()

    def contains(self, item: Expression[_T]) -> Predicate[_T]:
        return InContainer(item, self)

    @classmethod
    def range_literal(cls, r: range) -> Container[_T]:
        return RangeLiteral(r)

    @classmethod
    def sequence(cls, items: Sequence[Expression[_T]], dtype: type | None = None) -> Container[_T]:
        return ExpressionSequence(items, dtype)


class ContainerVisitor(Generic[_T, _U]):
    @abstractmethod
    def visit_range_literal(self, visited: RangeLiteral[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_expression_sequence(self, visited: ExpressionSequence[_T]) -> _U:
        raise NotImplementedError()


@dataclasses.dataclass
class RangeLiteral(Container[_T]):
    value: range

    @property
    def columns_required(self) -> Set[_T]:
        return frozenset()

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return True

    @property
    def dtype(self) -> type[int]:
        return int

    def visit(self, visitor: ContainerVisitor[_T, _U]) -> _U:
        return visitor.visit_range_literal(self)


@dataclasses.dataclass
class ExpressionSequence(Container[_T]):
    items: Sequence[Expression[_T]]
    dtype: type | None

    def visit(self, visitor: ContainerVisitor[_T, _U]) -> _U:
        return visitor.visit_expression_sequence(self)

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> Set[_T]:
        result: set[_T] = set()
        for item in self.items:
            result.update(item.columns_required)
        return result

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return all(item.is_supported_by(engine) for item in self.items)

    def __post_init__(self) -> None:
        for item in self.items:
            if item.has_window_function:
                raise RelationalAlgebraError(f"Cannot use window function expression {item} in container.")


@dataclasses.dataclass
class InContainer(Predicate[_T]):
    lhs: Expression[_T]
    rhs: Container[_T]

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_in_container(self)

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> Set[_T]:
        return self.lhs.columns_required | self.rhs.columns_required

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return self.lhs.is_supported_by(engine) and self.rhs.is_supported_by(engine)
