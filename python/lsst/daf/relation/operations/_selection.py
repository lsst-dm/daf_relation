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

from .._columns import _T, UniqueKey
from .._exceptions import ColumnError, EngineError
from .._relation import Relation

if TYPE_CHECKING:
    from .._engines import EngineTree
    from .._predicate import Predicate
    from .._relation_visitor import _U, RelationVisitor


@final
class Selection(Relation[_T]):
    def __init__(self, base: Relation[_T], predicates: frozenset[Predicate[_T]]):
        self.base = base
        self.predicates = predicates

    def __str__(self) -> str:
        return f"σ({self.base!s}, {{{', '.join(str(p) for p in self.predicates)}}})"

    @property
    def engine(self) -> EngineTree:
        return self.base.engine

    @property
    def columns(self) -> AbstractSet[_T]:
        return self.base.columns

    @property
    def unique_keys(self) -> AbstractSet[UniqueKey[_T]]:
        return self.base.unique_keys

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_selection(self)

    def checked_and_simplified(self, *, recursive: bool = True) -> Relation[_T]:
        base = self.base
        if recursive:
            base = base.checked_and_simplified(recursive=True)
        if not self.predicates:
            return base
        for p in self.predicates:
            if self.engine not in p.engine_state:
                raise EngineError(
                    f"Predicate {p} supports engine(s) {set(p.engine_state.keys())}, "
                    f"while relation has {self.engine}."
                )
            if not p.columns_required <= self.base.columns:
                raise ColumnError(
                    f"Predicate {p} for base relation {self.base} needs "
                    f"columns {p.columns_required - self.base.columns}."
                )
        match base:
            case Selection(base=base, predicates=predicates):
                return Selection(base, predicates | self.predicates)
        if base is self.base:
            return self
        return Selection(base, self.predicates)
