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
    "BaseExpression",
    "BaseLiteral",
    "BaseReference",
    "BaseFunction",
)

import dataclasses
from abc import abstractmethod
from collections.abc import Set
from typing import Generic, TypeVar

from lsst.utils.classes import cached_getter

from .._columns import _T
from .._engine import Engine

_A = TypeVar("_A", bound="BaseExpression")
_V = TypeVar("_V")


class BaseExpression(Generic[_T]):
    @property
    @abstractmethod
    def columns_required(self) -> Set[_T]:
        raise NotImplementedError()

    @abstractmethod
    def is_supported_by(self, engine: Engine) -> bool:
        raise NotImplementedError()


@dataclasses.dataclass
class BaseLiteral(BaseExpression[_T], Generic[_T, _V]):
    value: _V

    @property
    def columns_required(self) -> Set[_T]:
        return frozenset()

    def is_supported_by(self, engine: Engine) -> bool:
        return True


@dataclasses.dataclass
class BaseReference(BaseExpression[_T]):
    tag: _T

    @property
    def columns_required(self) -> Set[_T]:
        return {self.tag}

    def is_supported_by(self, engine: Engine) -> bool:
        return True


@dataclasses.dataclass
class BaseFunction(BaseExpression[_T], Generic[_T, _A]):

    name: str
    args: tuple[_A, ...]

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> Set[_T]:
        result: set[_T] = set()
        for arg in self.args:
            result.update(arg.columns_required)
        return result

    def is_supported_by(self, engine: Engine) -> bool:
        return engine.get_column_function(self.name) is not None and all(
            arg.is_supported_by(engine) for arg in self.args
        )
