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

__all__ = ("Leaf",)

from typing import TYPE_CHECKING, AbstractSet, Any, final

from ._columns import _T, UniqueKey, drop_covered_internal_unique_keys
from ._engines import EngineTag, EngineTree
from ._relation import Relation

if TYPE_CHECKING:
    from ._relation_visitor import _U, RelationVisitor


@final
class Leaf(Relation[_T]):
    def __init__(
        self,
        name: str,
        engine: EngineTag,
        general_state: dict[str, Any],
        engine_state: Any,
        columns: AbstractSet[_T],
        unique_keys: AbstractSet[UniqueKey[_T]],
    ):
        self.name = name
        self.general_state = general_state
        self._engine = EngineTree.build(engine)
        self.engine_state = engine_state
        self._columns = columns
        self._unique_keys = unique_keys

    @property
    def engine(self) -> EngineTree:
        return self._engine

    @property
    def columns(self) -> AbstractSet[_T]:
        return self._columns

    @property
    def unique_keys(self) -> AbstractSet[UniqueKey[_T]]:
        return self._unique_keys

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_leaf(self)

    def checked_and_simplified(
        self,
        *,
        recursive: bool = True,
        **kwargs: bool,
    ) -> Relation[_T]:
        self._check_unique_keys_in_columns()
        unique_keys = drop_covered_internal_unique_keys(self.unique_keys)
        if unique_keys != self.unique_keys:
            return Leaf(
                self.name, self.engine.tag, self.general_state, self.engine_state, self.columns, unique_keys
            )
        return self
