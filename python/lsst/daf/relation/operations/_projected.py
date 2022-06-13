# This file is part of daf_butler.
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

__all__ = ("ProjectedRelation",)

from typing import TYPE_CHECKING, AbstractSet, final

from lsst.utils.classes import cached_getter

from .._relation import Relation

if TYPE_CHECKING:
    from .._bounds import _B
    from .._column_tag import _T
    from .._relation_visitor import _U, RelationVisitor


@final
class ProjectedRelation(Relation[_T, _B]):
    def __init__(self, base: Relation[_T, _B], columns: AbstractSet[_T]):
        self._base = base
        self._columns = columns

    @property
    def columns(self) -> AbstractSet[_T]:
        return self._columns

    @property
    def bounds(self) -> _B:
        return self._base.bounds

    @property
    def connections(self) -> AbstractSet[frozenset[_T]]:
        return self._base.connections

    @property
    def is_full(self) -> bool:
        return self._base.is_full

    @property
    def is_materialized(self) -> bool:
        return self._base.is_materialized

    @property
    def doomed_by(self) -> AbstractSet[str]:
        return self._base.doomed_by

    def projected(self, columns: AbstractSet[_T]) -> Relation[_T, _B]:
        return ProjectedRelation(self._base, columns)

    def visit(self, visitor: RelationVisitor[_T, _B, _U]) -> _U:
        return visitor.visit_projected(self, self._base, self._columns)
