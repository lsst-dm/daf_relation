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

__all__ = ("SelectedRelation",)

from typing import TYPE_CHECKING, AbstractSet, final

from .._relation import Relation
from .._exceptions import MissingColumnError

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._predicate import Predicate
    from .._relation_visitor import _U, RelationVisitor


@final
class SelectedRelation(Relation[_T]):
    def __init__(self, base: Relation[_T], predicates: tuple[Predicate[_T], ...]):
        for p in predicates:
            if not p.columns_required <= self.columns:
                raise MissingColumnError(
                    f"Predicate {p} needs columns {set(p.columns_required - self.columns)}."
                )
        self._base = base
        self._predicates = predicates

    @property
    def columns(self) -> AbstractSet[_T]:
        return self._base.columns

    @property
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        return self._base.unique_keys

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_selected(self, self._base, self._predicates)
