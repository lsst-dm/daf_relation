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

__all__ = ("Projection",)

from typing import TYPE_CHECKING, AbstractSet, final

from lsst.utils.classes import cached_getter

from .._relation import Relation

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._engines import EngineTree
    from .._relation_visitor import _U, RelationVisitor


@final
class Projection(Relation[_T]):
    def __init__(self, base: Relation[_T], columns: frozenset[_T]):
        self.base = base
        self._columns = columns

    @property
    def engine(self) -> EngineTree:
        return self.base.engine

    @property
    def columns(self) -> frozenset[_T]:
        return self._columns

    @property  # type: ignore
    @cached_getter
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        return {keys for keys in self.base.unique_keys if keys.issubset(self._columns)}

    @property
    def doomed_by(self) -> AbstractSet[str]:
        return self.base.doomed_by

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_projection(self)
