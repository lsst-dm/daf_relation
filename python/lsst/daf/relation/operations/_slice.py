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

__all__ = ("Slice",)

from typing import TYPE_CHECKING, AbstractSet, final

from .._columns import _T, UniqueKey
from .._exceptions import ColumnError, EngineError
from .._relation import Relation

if TYPE_CHECKING:
    from .._engines import EngineTree
    from .._order_by_term import OrderByTerm
    from .._relation_visitor import _U, RelationVisitor


@final
class Slice(Relation[_T]):
    def __init__(self, base: Relation, order_by: tuple[OrderByTerm[_T], ...], offset: int, limit: int | None):
        self.base = base
        self.order_by = order_by
        self.offset = offset
        self.limit = limit

    @property
    def engine(self) -> EngineTree:
        return self.base.engine

    @property
    def columns(self) -> AbstractSet[_T]:
        return self.base.columns

    @property
    def unique_keys(self) -> AbstractSet[UniqueKey[_T]]:
        return self.base.unique_keys

    @property
    def doomed_by(self) -> AbstractSet[str]:
        result = self.base.doomed_by
        if self.limit == 0:
            result = set(result)
            result.add("Relation has been sliced to zero length.")
        return result

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_slice(self)

    def checked_and_simplified(self, *, recursive: bool = True) -> Relation[_T]:
        base = self.base
        if recursive:
            base = base.checked_and_simplified(recursive=True)
        if not self.order_by and not self.offset and self.limit is None:
            return base
        if self.engine.tag.options.sliced_sorts_only:
            if not self.offset and self.limit is None:
                raise EngineError(
                    "Cannot order a relation unless it is being sliced with nontrivial offset and/or limit."
                )
        if self.engine.tag.options.sorted_slices_only:
            if not self.order_by:
                raise EngineError("Cannot slice an unordered relation.")
        for o in self.order_by:
            if self.engine not in o.state:
                raise EngineError(
                    f"Order-by term {o} supports engine(s) {set(o.state.keys())}, "
                    f"while relation has {self.engine}."
                )
            if not o.columns_required <= self.base.columns:
                raise ColumnError(
                    f"Order-by term {o} for base relation {self.base} needs "
                    f"columns {o.columns_required - self.base.columns}."
                )
        if base is self.base:
            return self
        else:
            return Slice(base, self.order_by, self.offset, self.limit)
