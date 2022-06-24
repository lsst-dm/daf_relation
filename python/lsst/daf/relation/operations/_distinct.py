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

__all__ = ("Distinct",)

from typing import TYPE_CHECKING, AbstractSet, final

from .._columns import _T, UniqueKey, drop_covered_internal_unique_keys
from .._engines import EngineTree
from .._exceptions import RelationalAlgebraError
from .._relation import Relation

if TYPE_CHECKING:
    from .._relation_visitor import _U, RelationVisitor


@final
class Distinct(Relation[_T]):
    def __init__(self, base: Relation[_T], unique_keys: AbstractSet[UniqueKey[_T]]):
        self.base = base
        self._unique_keys = unique_keys

    def __str__(self) -> str:
        return f"distinct {self.base!s}"

    @property
    def engine(self) -> EngineTree:
        return self.base.engine

    @property
    def columns(self) -> AbstractSet[_T]:
        return self.base.columns

    @property
    def unique_keys(self) -> AbstractSet[UniqueKey[_T]]:
        return self._unique_keys

    @property
    def doomed_by(self) -> AbstractSet[str]:
        return self.base.doomed_by

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_distinct(self)

    def checked_and_simplified(self, recursive: bool = True) -> Relation[_T]:
        base = self.base
        if recursive:
            base = base.checked_and_simplified(recursive=True)
        if not self.unique_keys:
            raise RelationalAlgebraError(f"Distinct operation on {base} does not define any unique keys.")
        unique_keys = drop_covered_internal_unique_keys(self.unique_keys)
        if base.unique_keys:
            if base.unique_keys == self.unique_keys:
                return base
            else:
                raise RelationalAlgebraError(
                    f"Base relation {base} is already unique on keys {base.unique_keys}, "
                    f"but distinct operation declares it to be unique on {self.unique_keys}."
                )
        self._check_unique_keys_in_columns()
        if unique_keys == self.unique_keys and base is self.base:
            return self
        else:
            return Distinct(base, unique_keys)
