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
    "finish_join_row_iterable",
    "GeneralJoinRowIterable",
    "make_join_row_iterable",
    "UniqueIndexJoinRowIterable",
)

from typing import TYPE_CHECKING, AbstractSet, Iterator

from .._columns import _T, is_unique_key_covered
from ._row_iterable import RowIterable
from .selection import SelectionRowIterable

if TYPE_CHECKING:
    from .._engines import EngineTag
    from .._join_condition import JoinCondition
    from .._relation import Relation
    from ._typing import Row


def make_join_row_iterable(
    base_rows: RowIterable[_T],
    next_rows: RowIterable[_T],
    base_relation: Relation[_T],
    next_relation: Relation[_T],
    conditions: AbstractSet[JoinCondition[_T]],
) -> RowIterable[_T]:
    join_rows, matched_conditions = next_rows.try_join(next_relation, base_rows, base_relation, conditions)
    if join_rows is not None:
        return finish_join_row_iterable(base_relation.engine.tag, join_rows, conditions - matched_conditions)
    common_columns = frozenset(base_relation.columns & next_relation.columns)
    if is_unique_key_covered(common_columns, next_relation.unique_keys):
        return finish_join_row_iterable(
            base_relation.engine.tag,
            UniqueIndexJoinRowIterable(base_rows, next_rows, on_key=common_columns),
            conditions,
        )
    return finish_join_row_iterable(
        base_relation.engine.tag,
        GeneralJoinRowIterable(base_rows, next_rows, on_key=common_columns),
        conditions,
    )


def finish_join_row_iterable(
    engine: EngineTag,
    base: RowIterable[_T],
    missing_conditions: AbstractSet[JoinCondition[_T]],
) -> RowIterable[_T]:
    if not missing_conditions:
        return base
    return SelectionRowIterable(base, tuple(c.state[engine] for c in missing_conditions))


class UniqueIndexJoinRowIterable(RowIterable[_T]):
    def __init__(self, base: RowIterable[_T], next: RowIterable[_T], on_key: frozenset[_T]):
        self.base = base
        self.next = next
        self.on_key = on_key

    def __iter__(self) -> Iterator[Row[_T]]:
        index = self.next.with_unique_index(self.on_key).get_unique_index(self.on_key)
        for base_row in self.base:
            key = tuple(base_row[k] for k in self.on_key)
            if (index_row := index.get(key)) is not None:
                yield {**base_row, **index_row}


class GeneralJoinRowIterable(RowIterable[_T]):
    def __init__(self, base: RowIterable[_T], next: RowIterable[_T], on_key: frozenset[_T]):
        self.base = base
        self.next = next
        self.on_key = on_key

    def __iter__(self) -> Iterator[Row[_T]]:
        raise NotImplementedError()
