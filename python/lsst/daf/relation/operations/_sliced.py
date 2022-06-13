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

from typing import TYPE_CHECKING, AbstractSet, final

from .._relation import Relation
from .._exceptions import MissingColumnError

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._order_by_term import OrderByTerm
    from .._relation_visitor import _U, RelationVisitor


@final
class SlicedRelation(Relation[_T]):
    def __init__(self, base: Relation, order_by: tuple[OrderByTerm[_T], ...], offset: int, limit: int | None):
        # TypeError may seem strange below, but it's what Python usually raises
        # when you pass an invalid combination of arguments to a function.
        if not order_by:
            raise TypeError(
                "Cannot slice an unordered relation; to obtain an arbitrary "
                "set of result rows from an unordered relation, pass offset "
                "and/or limit to_sql_executable when executing it."
            )
        if not offset and limit is None:
            raise TypeError(
                "Cannot order a relation unless it is being sliced with "
                "nontrivial offset and/or limit; to obtain ordered rows from "
                "a relation, pass order_by to to_sql_executable when "
                "executing it."
            )

        for t in order_by:
            if not t.columns_required <= self.columns:
                raise MissingColumnError(
                    f"OrderByTerm {t} needs columns {set(t.columns_required - self.columns)}."
                )
        self._base = base
        self._order_by = order_by
        self._offset = offset
        self._limit = limit

    @property
    def columns(self) -> AbstractSet[_T]:
        return self._base.columns

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

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_sliced(self, self._base, self._order_by, self._offset, self._limit)
