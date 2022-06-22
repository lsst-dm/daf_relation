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

__all__ = ()

import enum
import itertools
from abc import abstractmethod
from typing import (
    AbstractSet,
    Any,
    Callable,
    Collection,
    Generic,
    Iterator,
    Literal,
    Mapping,
    Sequence,
    Tuple,
)

from .._column_tag import _T
from .._engines import EngineTag
from .._join_condition import JoinCondition
from .._predicate import Predicate
from .._relation import Relation

Row = Mapping[_T, Any]
UniqueIndex = Mapping[Tuple[Any, ...], Row[_T]]
SequenceIndex = Mapping[Tuple[Any, ...], Sequence[Row[_T]]]


class Materialization(enum.Enum):
    NONE = 0
    DERIVED = 1
    DIRECT = 2
    GENERATED = 3

    def __ge__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class RowIterable(Generic[_T]):
    @abstractmethod
    def __iter__(self) -> Iterator[Row[_T]]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def materialization(self) -> Materialization:
        raise NotImplementedError()

    def with_unique_index(self, key_columns: frozenset[_T]) -> UniqueIndexedRowIterable[_T]:
        new_index: dict[tuple[Any, ...], Row[_T]] = {}
        self._build_unique_index(key_columns, new_index)
        return RowContainer(new_index.values(), {key_columns: new_index})

    def _build_unique_index(
        self, key_columns: frozenset[_T], new_index: dict[tuple[Any, ...], Row[_T]]
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
    def get_unique_index(self, key_columns: frozenset[_T]) -> UniqueIndex[_T]:
        raise NotImplementedError()


class RowContainer(UniqueIndexedRowIterable[_T]):
    def __init__(
        self, rows: Collection[Row[_T]], unique_indexes: dict[frozenset[_T], UniqueIndex[_T]] | None = None
    ):
        self.rows = rows
        self._unique_indexes = dict(unique_indexes) if unique_indexes is not None else {}

    def __iter__(self) -> Iterator[Row[_T]]:
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    @property
    def materialization(self) -> Literal[Materialization.DIRECT]:
        return Materialization.DIRECT

    def with_unique_index(self, key_columns: frozenset[_T]) -> RowContainer[_T]:
        new_index: dict[tuple[Any, ...], Row[_T]] = {}
        if self._unique_indexes.setdefault(key_columns, new_index) is new_index:
            self._build_unique_index(key_columns, new_index)
        return self

    def get_unique_index(self, key_columns: frozenset[_T]) -> UniqueIndex[_T]:
        new_index: dict[tuple[Any, ...], Row[_T]] = {}
        if (existing_index := self._unique_indexes.setdefault(key_columns, new_index)) is not new_index:
            return existing_index
        else:
            self._build_unique_index(key_columns, new_index)
            return new_index


class ProjectionRowIterable(RowIterable[_T]):
    def __init__(self, base: RowIterable[_T], columns: tuple[_T, ...]):
        self.base = base
        self.columns = columns

    def __iter__(self) -> Iterator[Row[_T]]:
        return ({k: row[k] for k in self.columns} for row in self.base)

    @property
    def materialization(self) -> Materialization:
        return min(self.base.materialization, Materialization.DERIVED)


class JoinConditionSelectionRowIterable(RowIterable[_T]):
    def __init__(self, base: RowIterable[_T], conditions: tuple[Callable[[Row[_T], Row[_T]], bool], ...]):
        self.base = base
        self.conditions = conditions

    def __iter__(self) -> Iterator[Row[_T]]:
        return (row for row in self.base if all(jc(row, row) for jc in self.conditions))

    @property
    def materialization(self) -> Materialization:
        return min(self.base.materialization, Materialization.DERIVED)


class SelectionRowIterable(RowIterable[_T]):
    def __init__(self, base: RowIterable[_T], predicates: tuple[Callable[[Row[_T]], bool], ...]):
        self.base = base
        self.predicates = predicates

    def __iter__(self) -> Iterator[Row[_T]]:
        return (row for row in self.base if all(p(row) for p in self.predicates))

    @property
    def materialization(self) -> Materialization:
        return min(self.base.materialization, Materialization.DERIVED)


class ChainRowIterable(RowIterable[_T]):
    def __init__(self, chain: Sequence[RowIterable[_T]]):
        self.chain = chain

    def __iter__(self) -> Iterator[Row[_T]]:
        return itertools.chain.from_iterable(self.chain)

    @property
    def materialization(self) -> Materialization:
        return min((b.materialization for b in self.chain), default=Materialization.DIRECT)


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

    @property
    def materialization(self) -> Materialization:
        return min(self.base.materialization, self.next.materialization, Materialization.DERIVED)


class GeneralJoinRowIterable(RowIterable[_T]):
    def __init__(self, base: RowIterable[_T], next: RowIterable[_T], on_key: frozenset[_T]):
        self.base = base
        self.next = next
        self.on_key = on_key

    def __iter__(self) -> Iterator[Row[_T]]:
        raise NotImplementedError()

    @property
    def materialization(self) -> Materialization:
        return min(self.base.materialization, self.next.materialization, Materialization.DERIVED)
