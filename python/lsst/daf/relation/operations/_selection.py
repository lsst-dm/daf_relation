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

__all__ = ("Selection",)

from typing import TYPE_CHECKING, AbstractSet, final

from .._exceptions import MissingColumnError
from .._relation import Relation

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._engine_tag import EngineTag
    from .._predicate import Predicate
    from .._relation_visitor import _U, RelationVisitor


@final
class Selection(Relation[_T]):
    def __init__(self, base: Relation[_T], predicates: tuple[Predicate[_T], ...]):
        for p in predicates:
            if not p.columns_required <= self.columns:
                raise MissingColumnError(
                    f"Predicate {p} needs columns {set(p.columns_required - self.columns)}."
                )
        self.base = base
        self.predicates = predicates

    @property
    def engine(self) -> EngineTag:
        return self.base.engine

    @property
    def columns(self) -> AbstractSet[_T]:
        return self.base.columns

    @property
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        return self.base.unique_keys

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_selection(self)
