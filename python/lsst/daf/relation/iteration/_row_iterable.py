# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "RowIterable",
    "RowIterableLeaf",
    "UniqueIndexedRowIterable",
)

from abc import abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Any, Generic, Iterator

from .._columns import _T, UniqueKey
from .._engines import EngineTag
from .._join_condition import JoinCondition
from .._predicate import Predicate
from .._relation import Relation
from .._leaf import Leaf

if TYPE_CHECKING:
    from ._typing import Row, UniqueIndex


class RowIterable(Generic[_T]):
    @abstractmethod
    def __iter__(self) -> Iterator[Row[_T]]:
        raise NotImplementedError()

    def with_unique_index(self, key_columns: UniqueKey[_T]) -> UniqueIndexedRowIterable[_T]:
        from ._row_container import RowContainer

        new_index: dict[tuple[Any, ...], Row[_T]] = {}
        self._build_unique_index(key_columns, new_index)
        return RowContainer(new_index.values(), {key_columns: new_index})

    def _build_unique_index(
        self, key_columns: UniqueKey[_T], new_index: dict[tuple[Any, ...], Row[_T]]
    ) -> None:
        for row in self:
            key = tuple(row[c] for c in key_columns)
            new_index[key] = row

    def try_selection(
        self, engine: EngineTag, predicates: AbstractSet[Predicate[_T]]
    ) -> tuple[RowIterable[_T], AbstractSet[Predicate[_T]]]:
        return (self, frozenset())

    def try_join(
        self,
        self_relation: Relation[_T],
        base: RowIterable[_T],
        base_relation: Relation[_T],
        conditions: AbstractSet[JoinCondition[_T]],
    ) -> tuple[RowIterable[_T] | None, AbstractSet[JoinCondition[_T]]]:
        return None, frozenset()


class UniqueIndexedRowIterable(RowIterable[_T]):
    @abstractmethod
    def get_unique_index(self, key_columns: UniqueKey[_T]) -> UniqueIndex[_T]:
        raise NotImplementedError()


class RowIterableLeaf(Leaf[_T]):
    def __init__(self, *args: Any, rows: RowIterable):
        super().__init__(*args)
        self.rows = rows
