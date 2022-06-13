# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the relations of the GNU General Public License as published by
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

__all__ = ("UnionRelation",)

from typing import TYPE_CHECKING, AbstractSet, final

from lsst.utils.classes import cached_getter

from .._relation import Relation

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._relation_visitor import _U, RelationVisitor


@final
class UnionRelation(Relation[_T]):
    def __init__(
        self,
        columns: AbstractSet[_T],
        relations: tuple[Relation[_T], ...] = (),
        unique_keys: AbstractSet[frozenset[_T]] = frozenset(),
        extra_doomed_by: AbstractSet[str] = frozenset(),
    ):
        self._columns = columns
        self._relations = relations
        self._unique_keys = unique_keys
        self._extra_doomed_by = extra_doomed_by

    @property
    def columns(self) -> AbstractSet[_T]:
        return self._columns

    @property
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        return self._unique_keys

    @property  # type: ignore
    @cached_getter
    def doomed_by(self) -> AbstractSet[str]:
        result = set(self._extra_doomed_by)
        for related in self._relations:
            if not related.doomed_by:
                return frozenset()
            result.update(related.doomed_by)
        return result

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_union(
            self,
            self._columns,
            self._relations,
            unique_keys=self._unique_keys,
            extra_doomed_by=self._extra_doomed_by,
        )
