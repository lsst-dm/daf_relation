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

__all__ = ("RowContainer", "RowContainerLeaf")

from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any, Collection

from .._columns import _T, UniqueKey
from ._row_iterable import RowIterableLeaf, UniqueIndexedRowIterable

if TYPE_CHECKING:
    from ._typing import Row, UniqueIndex


class RowContainer(UniqueIndexedRowIterable[_T]):
    def __init__(
        self, rows: Collection[Row[_T]], unique_indexes: dict[UniqueKey[_T], UniqueIndex[_T]] | None = None
    ):
        self.rows = rows
        self._unique_indexes = dict(unique_indexes) if unique_indexes is not None else {}

    def __iter__(self) -> Iterator[Row[_T]]:
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def with_unique_index(self, key_columns: UniqueKey[_T]) -> RowContainer[_T]:
        new_index: dict[tuple[Any, ...], Row[_T]] = {}
        if self._unique_indexes.setdefault(key_columns, new_index) is new_index:
            self._build_unique_index(key_columns, new_index)
        return self

    def get_unique_index(self, key_columns: UniqueKey[_T]) -> UniqueIndex[_T]:
        new_index: dict[tuple[Any, ...], Row[_T]] = {}
        if (existing_index := self._unique_indexes.setdefault(key_columns, new_index)) is not new_index:
            return existing_index
        else:
            self._build_unique_index(key_columns, new_index)
            return new_index


class RowContainerLeaf(RowIterableLeaf[_T]):
    def __init__(self, *args: Any, rows: RowContainer[_T]):
        super().__init__(*args, rows=rows)

    def write_extra_to_mapping(self) -> Mapping[str, Any]:
        return {"rows": [dict(row) for row in self.rows]}
