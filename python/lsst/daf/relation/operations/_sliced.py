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

__all__ = ("SlicedRelation",)

from typing import TYPE_CHECKING, AbstractSet, Iterable, final

from .._relation import Relation

if TYPE_CHECKING:
    from .._bounds import _B
    from .._column_tag import _T
    from .._order_by_term import OrderByTerm
    from .._relation_visitor import _U, RelationVisitor


@final
class SlicedRelation(Relation[_T, _B]):
    def __init__(self, base: Relation, order_by: tuple[OrderByTerm[_T], ...], offset: int, limit: int | None):
        self._base = base
        self._order_by = order_by
        self._offset = offset
        self._limit = limit

    @property
    def columns(self) -> AbstractSet[_T]:
        return self._base.columns

    @property
    def bounds(self) -> _B:
        return self._base.bounds

    @property
    def connections(self) -> AbstractSet[frozenset[_T]]:
        return self._base.connections

    @property
    def is_full(self) -> bool:
        return False

    @property
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        return self._base.unique_keys

    @property
    def doomed_by(self) -> AbstractSet[str]:
        result = self._base.doomed_by
        if self._limit == 0:
            result = set(result)
            result.add("Relation has been sliced to zero length.")
        return result

    def sliced(
        self, order_by: Iterable[OrderByTerm[_T]], offset: int = 0, limit: int | None = None
    ) -> Relation:
        if not order_by:
            order_by = self._order_by
        else:
            order_by = list(order_by)
            order_by.extend(self._order_by)
        combined_offset = self._offset + offset
        if limit is not None:
            combined_limit: int | None
            if self._limit is not None:
                original_stop = self._offset + self._limit
                new_stop = offset + limit
                combined_stop = min(original_stop, new_stop)
                combined_limit = max(combined_stop - offset, 0)
            else:
                combined_limit = self._limit
        return SlicedRelation(self._base, tuple(order_by), offset=combined_offset, limit=combined_limit)

    def visit(self, visitor: RelationVisitor[_T, _B, _U]) -> _U:
        return visitor.visit_sliced(self, self._base, self._order_by, self._offset, self._limit)
