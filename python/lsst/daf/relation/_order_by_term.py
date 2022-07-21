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

__all__ = ("OrderByTerm",)

from abc import abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING, Any, Generic

from ._columns import _T

if TYPE_CHECKING:
    from ._engines import Engine
    from ._serialization import DictWriter


class OrderByTerm(Generic[_T]):
    @property
    @abstractmethod
    def columns_required(self) -> Set[_T]:
        """The columns required to compute this expression
        (`~collections.abc.Set`).
        """
        raise NotImplementedError()

    @abstractmethod
    def supports_engine(self, engine: Engine) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def reversed(self) -> OrderByTerm[_T]:
        raise NotImplementedError()

    @abstractmethod
    def serialize(self, writer: DictWriter[_T]) -> dict[str, Any]:
        raise NotImplementedError()
