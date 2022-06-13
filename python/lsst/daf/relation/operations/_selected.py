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

from lsst.utils.classes import cached_getter

from .._relation import Relation

if TYPE_CHECKING:
    from .._bounds import _B
    from .._column_tag import _T
    from .._predicate import Predicate
    from .._relation_visitor import _U, RelationVisitor


@final
class SelectedRelation(Relation[_T, _B]):
    def __init__(self, base: Relation[_T, _B], predicates: tuple[Predicate[_T, _B], ...]):
        self._base = base
        self._predicates = predicates

    @property
    def columns(self) -> AbstractSet[_T]:
        return self._base.columns

    @property  # type: ignore
    @cached_getter
    def bounds(self) -> _B:
        return self._base.bounds.intersection(*[p.bounds for p in self._predicates])

    @property
    def connections(self) -> AbstractSet[frozenset[_T]]:
        return self._base.connections

    @property
    def is_full(self) -> bool:
        return False

    @property
    def is_unique(self) -> bool:
        return self._base.is_unique

    def selected(self, *predicates: Predicate) -> Relation:
        return SelectedRelation(self._base, self._predicates + predicates)

    def visit(self, visitor: RelationVisitor[_T, _B, _U]) -> _U:
        return visitor.visit_selected(self, self._base, self._predicates)
