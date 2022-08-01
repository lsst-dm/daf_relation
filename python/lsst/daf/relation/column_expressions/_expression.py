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

from lsst.utils.classes import cached_getter
from lsst.utils.sets.ellipsis import EllipsisType
from lsst.utils.sets.unboundable import FrozenUnboundableSet, UnboundableSet

from .._columns import _T
from .._exceptions import RelationalAlgebraError
from ._predicate import Predicate, PredicateVisitor

if TYPE_CHECKING:
    from .._engine import Engine

_U = TypeVar("_U")


class Expression(Generic[_T]):
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
    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        raise NotImplementedError()

    @property
    @abstractmethod
    def has_window_function(self) -> bool:
        raise NotImplementedError()

    @classmethod
    def reference(cls, tag: _T, dtype: type | None = None) -> Expression[_T]:
        return Reference(tag, dtype)

    @classmethod
    def literal(cls, value: Any, dtype: type | None = None) -> Expression[_T]:
        return Literal(value, dtype)

    @classmethod
    def function(
        cls,
        name: str,
        *args: Expression[_T],
        dtype: type | None = None,
        supporting_engines: Set[Engine[_T] | type[Engine[_T]]]
        | EllipsisType
        | UnboundableSet[Engine[_T] | type[Engine[_T]]] = FrozenUnboundableSet.full,
        is_window_function: bool = False,
    ) -> Expression[_T]:
        return Function(
            name,
            args,
            dtype,
            supporting_engines=FrozenUnboundableSet.coerce(supporting_engines),
            is_window_function=is_window_function,
        )

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
        return PredicateFunction[_T](name, (self,) + args)


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
class Literal(Expression[_T]):
    value: Any
    dtype: type | None

    @property
    def columns_required(self) -> Set[_T]:
        return frozenset()

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return True

    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_literal(self)

    @property
    def has_window_function(self) -> bool:
        return False


@dataclasses.dataclass
class Reference(Expression[_T]):
    tag: _T
    dtype: type | None

    @property
    def columns_required(self) -> Set[_T]:
        return frozenset()

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return True

    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_reference(self)

    @property
    def has_window_function(self) -> bool:
        return False


@dataclasses.dataclass
class Function(Expression[_T]):

    name: str
    args: tuple[Expression[_T], ...]
    dtype: type | None
    supporting_engines: FrozenUnboundableSet[Engine[_T] | type[Engine[_T]]] = FrozenUnboundableSet.full
    is_window_function: bool = dataclasses.field(default=False)

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> Set[_T]:
        result: set[_T] = set()
        for arg in self.args:
            result.update(arg.columns_required)
        return result

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return (engine in self.supporting_engines or type(engine) in self.supporting_engines) and all(
            arg.is_supported_by(engine) for arg in self.args
        )

    @property
    def has_window_function(self) -> bool:
        return self.is_window_function or any(arg.has_window_function for arg in self.args)

    def visit(self, visitor: ExpressionVisitor[_T, _U]) -> _U:
        return visitor.visit_function(self)


@dataclasses.dataclass
class PredicateFunction(Predicate[_T]):

    name: str
    args: tuple[Expression[_T], ...]
    supporting_engines: FrozenUnboundableSet[Engine[_T] | type[Engine[_T]]] = FrozenUnboundableSet.full

    def __post_init__(self) -> None:
        for arg in self.args:
            if arg.has_window_function:
                raise RelationalAlgebraError(f"Cannot use window function expression {arg} in predicate.")

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> Set[_T]:
        result: set[_T] = set()
        for arg in self.args:
            result.update(arg.columns_required)
        return result

    def is_supported_by(self, engine: Engine[_T]) -> bool:
        return (engine in self.supporting_engines or type(engine) in self.supporting_engines) and all(
            arg.is_supported_by(engine) for arg in self.args
        )

    def visit(self, visitor: PredicateVisitor[_T, _U]) -> _U:
        return visitor.visit_predicate_function(self)
