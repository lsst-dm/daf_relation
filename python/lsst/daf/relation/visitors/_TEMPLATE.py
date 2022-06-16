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

__all__ = ("_TEMPLATE",)

from typing import TYPE_CHECKING, AbstractSet

from .. import operations
from .._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._engines import EngineTag
    from .._leaf import Leaf
    from .._relation import Relation


class _TEMPLATE(RelationVisitor[_T, Relation[_T]]):
    def __init__(self, engines: AbstractSet[EngineTag]):
        self.engines = engines

    def visit_leaf(self, visited: Leaf[_T]) -> Relation[_T]:
        return visited

    def visit_join(self, visited: operations.Join[_T]) -> Relation[_T]:
        return visited

    def visit_projection(self, visited: operations.Projection[_T]) -> Relation[_T]:
        return visited

    def visit_selection(self, visited: operations.Selection[_T]) -> Relation[_T]:
        return visited

    def visit_slice(self, visited: operations.Slice[_T]) -> Relation[_T]:
        return visited

    def visit_transfer(self, visited: operations.Transfer[_T]) -> Relation[_T]:
        return visited

    def visit_union(self, visited: operations.Union[_T]) -> Relation[_T]:
        return visited
