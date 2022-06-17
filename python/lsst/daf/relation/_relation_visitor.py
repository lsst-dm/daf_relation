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

__all__ = ("RelationVisitor",)

from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from . import operations
    from ._column_tag import _T
    from ._leaf import Leaf

_U = TypeVar("_U", covariant=True)


class RelationVisitor(Generic[_T, _U]):
    @abstractmethod
    def visit_leaf(self, visited: Leaf[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_join(self, visited: operations.Join[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_projection(self, visited: operations.Projection[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_selection(self, visited: operations.Selection[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_slice(self, visited: operations.Slice[_T]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_transfer(self, visited: operations.Transfer) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_union(self, visited: operations.Union[_T]) -> _U:
        raise NotImplementedError()
