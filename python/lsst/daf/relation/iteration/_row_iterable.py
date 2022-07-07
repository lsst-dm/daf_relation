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
    "IndexedRowIterable",
    "RowCollection",
    "RowIterable",
    "RowIterableLeaf",
    "SerializableRowIterableLeaf",
)

import itertools
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any, Generic

from .._columns import _T, UniqueKey
from .._join_condition import JoinCondition
from .._leaf import Leaf
from .._order_by_term import OrderByTerm
from .._predicate import Predicate
from .._relation import Relation

if TYPE_CHECKING:
    from .._engines import EngineTag
    from .typing import GeneralIndex, IndexKey, Row, UniqueIndex


class RowIterable(Generic[_T]):
    """An abstract base class for iterables that use mappings for rows."""

    @abstractmethod
    def __iter__(self) -> Iterator[Row[_T]]:
        raise NotImplementedError()

    def with_general_index(self, index_columns: frozenset[_T]) -> IndexedRowIterable[_T]:
        """Return a version of this iterable with a non-unique index on the
        given set of columns.

        Parameters
        ----------
        index_columns : `frozenset`
            Set of columns to index by.

        Returns
        -------
        indexed : `IndexedRowIterable`
            A new row iterable with a non-unique index.  May be ``self``.

        Notes
        -----
        This is the first step in a two-step process for creating an index
        index (a `Mapping` view of a row iterable, with a `tuple` of column
        values for keys and `Sequence` of `Row` mappings for values).  The next
        step is `IndexedRowIterable.get_general_index`.  We've split it into
        two methods to reduce the number of times an iterable has to be
        iterated over to build multiple indexes:

        - for a lazy iterable (i.e. the base class `RowIterable` and most
          subclasses) we iterate once to both build the index and create a new
          `IndexedRowIterable` in a single pass;

        - for an existing `IndexedRowIterable` that already has that index, we
          just use it, because its `with_unique_index` method just returns
          ``self``;

        - for an existing `IndexedRowIterable` that doesn't have that index, we
          can create it with (presumably faster) iteration over that iterable
          instead of the original iterable.
        """
        new_index: defaultdict[IndexKey, list[Row[_T]]] = defaultdict(list)
        self._build_general_index(self, index_columns, new_index)
        return IndexedRowIterable({index_columns: new_index})

    def with_unique_index(self, index_columns: UniqueKey[_T]) -> RowCollection[_T]:
        """Return a version of this iterable with a unique index on the given
        key.

        Parameters
        ----------
        index_columns : `.UniqueKey`
            Set of columns with unique values across all rows.

        Returns
        -------
        indexed : `RowCollection`
            A new row collection with a unique index.  May be ``self``.  This
            is a `RowCollection` because the a unique index's ``values`` view
            is always itself a viable collection when an existing one does not
            exist.

        Notes
        -----
        See `with_general_index` for why this is a two-step process.
        """
        return RowCollection.build_with_unique_index(self, index_columns)

    def try_join(
        self,
        self_relation: Relation[_T],
        base: RowIterable[_T],
        base_relation: Relation[_T],
        conditions: Set[JoinCondition[_T]],
    ) -> tuple[RowIterable[_T] | None, Set[JoinCondition[_T]]]:
        """Hook for performing custom joins on this iterable.

        This can be overridden by subclasses to customize how join operations
        are implemented.  Note that it the leaf relation this iterable holds
        must be directly held by a join operation for it to be used.

        Parameters
        ----------
        self_relation : `.Relation`
            The relation associated with this set of rows.  This is always the
            *second* member in a join operation's two relations.
        base : `RowIterable`
            The row iterable this object is being joined to.
        base_relation : `.Relation`
            The relation associated with `base`.
        conditions : `~collections.abc.Set` [ `.JoinCondition` ]
            Custom join conditions for which ``required_columns[0]`` is a
            subset of ``base_relation.columns`` and `required_columns[1]`` is a
            subset of ``self_relation.columns``.

        Returns
        -------
        join_rows : `RowIterable` or `None`
            A `RowIterable` that implements this join, or `None` if there is no
            special handling for this join.
        matched_conditions : `~collections.abc.Set` [ `.JoinCondition` ]
            Join conditions included in ``join_rows``.  Any that remain will
            be applied by assuming they have `JoinCondition.engine_state` for
            this engine set to a callable with the  `JoinConditionState`
            signature.
        """
        return None, frozenset()

    def try_selection(self, predicates: Set[Predicate[_T]]) -> tuple[RowIterable[_T], Set[Predicate[_T]]]:
        """Hook for performing custom selections on this iterable.

        This can be overridden by subclasses to customize how selection
        operations are implemented.  Note that it the leaf relation this
        iterable holds must be directly held by a selection operation for it to
        be used.

        Parameters
        ----------
        predicates : `~collections.abc.Set` [ `.Predicate` ]
            Predicates to attempt to apply.

        Returns
        -------
        rows : `RowIterable`
            Rows that may have some predicates already applied.  Should be
            ``self`` if no predicates are applied.
        matching_predicates : `~collections.abc.Set` [ `.Predicate` ]
            Set of predicates actually included in ``rows``.  Any that remain
            will be applied by assuming they have `Predicate.engine_state`
            for this engine set to a callable with the `PredicateState`
            signature.
        """
        return (self, frozenset())

    def try_slice(
        self, order_by: Sequence[OrderByTerm[_T]], offset: int, limit: int | None
    ) -> RowIterable[_T] | None:
        """Hook for performing custom selections and sorting on this iterable.

        Parameters
        ----------
        order_by : `~collections.abc.Sequence` [ `.OrderByTerm` ]
            Sequence of order-by terms to apply.
        offset : `int`
            Index (from 0) of the first row to include in the result.
        limit : `int` or `None`
            Maximum number of rows to include in the result, or `None` for no
            limit.

        Returns
        -------
        rows : `RowIterable` or `None`
            Rows with sorting and slicing applied, or `None` for no special
            handling.
        """
        return None

    @staticmethod
    def _build_general_index(
        rows: Iterable[Row[_T]], index_columns: frozenset[_T], new_index: defaultdict[IndexKey, list[Row[_T]]]
    ) -> None:
        """Internal helper method to create non-unique index mappings.

        Parameters
        ----------
        rows : `Iterable` [ `Row` ]
            Input rows to process.
        index_columns : `frozenset`
            Set of columns to build an index over.
        new_index : `defaultdict` [ `IndexKey`, `list` [ `Row` ] ]
            Empty mapping to populate.
        """
        for row in rows:
            key = tuple(row[c] for c in index_columns)
            new_index[key].append(row)

    @staticmethod
    def _build_unique_index(
        rows: Iterable[Row[_T]], index_columns: UniqueKey[_T], new_index: dict[IndexKey, Row[_T]]
    ) -> None:
        """Internal helper method to create unique index mappings.

        Parameters
        ----------
        rows : `Iterable` [ `Row` ]
            Input rows to process.
        index_columns : `.UniqueKey`
            Set of columns that are unique over all rows (or, if duplicates
            occur, they are fully-row duplicates that may be discarded).
        new_index : `dict` [ `IndexKey`, `Row` ]
            Empty mapping to populate.
        """
        for row in rows:
            key = tuple(row[c] for c in index_columns)
            new_index[key] = row


class IndexedRowIterable(RowIterable[_T]):
    """A `RowIterable` that can hold general (non-unique) indexes.

    Parameters
    ----------
    general_indexes :  `dict` [ `frozenset`, `GeneralIndex` ]
        Nested mapping of non-unique indexes.  Outermost keys are sets of
        column tags, and each value is itself a mapping whose keys are a tuple
        of column values (in the same order as the set of column tags), with
        sequences of rows as the inner values.  Must have at least one entry
        unless a reimplementation of ``__iter__`` is provided by a derived
        class.
    """

    def __init__(self, general_indexes: dict[frozenset[_T], GeneralIndex[_T]]):
        self._general_indexes = general_indexes

    def __iter__(self) -> Iterator[Row[_T]]:
        first_index = next(iter(self._general_indexes.values()))
        return itertools.chain.from_iterable(first_index.values())

    def with_unique_index(self, index_columns: UniqueKey[_T]) -> RowCollection[_T]:
        # Docstring inherited.
        new_index: dict[IndexKey, Row[_T]] = {}
        self._build_unique_index(self, index_columns, new_index)
        return RowCollection(
            new_index.values(),
            unique_indexes={index_columns: new_index},
            # We reimplement the base class method just so we can transfer
            # our general_indexes.
            general_indexes=dict(self._general_indexes),
        )

    def get_general_index(self, index_columns: frozenset[_T]) -> GeneralIndex[_T]:
        """Return a non-unique index for the given columns, creating it if
        necessary.

        Parameters
        ----------
        index_columns : `frozenset`
            Set of columns to build an index on.

        Returns
        -------
        index : `Mapping` [ `IndexKey`, `Row` ]
            `GeneralIndex` mapping whose keys are tuples of column values (in
            the same order as ``index_columns``) with sequences of rows as
            values.
        """
        new_index: defaultdict[IndexKey, list[Row[_T]]] = defaultdict(list)
        if (existing_index := self._general_indexes.setdefault(index_columns, new_index)) is not new_index:
            return existing_index
        else:
            self._build_general_index(self, index_columns, new_index)
            return new_index


class RowCollection(IndexedRowIterable[_T]):
    """A `RowIterable` backed by a regular Python collection.

    Parameters
    ----------
    rows : `Collection` [ `Row` ]
        Collection for rows, with each row a mapping keyed by `.ColumnTag`.
    unique_indexes : `dict` [ `.UniqueKey`, `UniqueIndex` ]
        Nested mapping of unique indexes.  Outermost keys are sets of column
        tags, and each value is itself a mapping whose keys are a tuple of
        column values (in the same order as the set of column tags), with the
        full row mapping the inner values.
    general_indexes :  `dict` [ `frozenset`, `GeneralIndex` ]
        Nested mapping of non-unique indexes.  Has the same form as
        ``unique_indexes``, but the innermost mapping values are sequences of
        rows, not individual rows.
    """

    def __init__(
        self,
        rows: Collection[Row[_T]],
        unique_indexes: dict[UniqueKey[_T], UniqueIndex[_T]] | None = None,
        general_indexes: dict[frozenset[_T], GeneralIndex[_T]] | None = None,
    ):
        self.rows = rows
        self._unique_indexes = dict(unique_indexes) if unique_indexes is not None else {}
        self._general_indexes = dict(general_indexes) if general_indexes is not None else {}

    @staticmethod
    def build_with_unique_index(rows: Iterable[Row[_T]], index_columns: UniqueKey[_T]) -> RowCollection[_T]:
        """Build a row collection at the same time as a unique index.

        This allows the index mapping's ``values`` view to be used as the main
        collection, with only a single pass through the given iterable.

        Parameters
        ----------
        rows : `Iterable` [ `Row` ]
            Iterable to build the index and collection from.
        index_columns : `.UniqueKey`
            Columns to index on.

        Returns
        -------
        row_collection : `
        """
        new_index: dict[IndexKey, Row[_T]] = {}
        RowIterable._build_unique_index(rows, index_columns, new_index)
        return RowCollection(new_index.values(), unique_indexes={index_columns: new_index})

    def __iter__(self) -> Iterator[Row[_T]]:
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def with_general_index(self, index_columns: frozenset[_T]) -> RowCollection[_T]:
        # Docstring inherited.
        return self

    def with_unique_index(self, index_columns: UniqueKey[_T]) -> RowCollection[_T]:
        # Docstring inherited.
        return self

    def get_unique_index(self, index_columns: UniqueKey[_T]) -> UniqueIndex[_T]:
        """Return a unique index for the given columns, creating it if
        necessary.

        Parameters
        ----------
        index_columns : `.UniqueKey`
            Set of columns that are unique over all rows (or, if duplicates
            occur, they are fully-row duplicates that may be discarded).

        Returns
        -------
        index : `Mapping` [ `IndexKey`, `Row` ]
            `UniqueIndex` mapping whose keys are tuples of column values (in
            the same order as ``index_columns``) with full-row mappings as
            values.
        """
        new_index: dict[IndexKey, Row[_T]] = {}
        if (existing_index := self._unique_indexes.setdefault(index_columns, new_index)) is not new_index:
            return existing_index
        else:
            self._build_unique_index(self, index_columns, new_index)
            return new_index


class RowIterableLeaf(Leaf[_T]):
    """A `Leaf` relation for the native iteration engine, backed by a lazy
    iterable.

    Parameters
    ----------
    name : `str`
        Name for the relation.  This is used to implement `str` and is part of
        the serialized form of a relation (and hence `repr` as well), but is
        otherwise ignored.
    engine : `EngineTag`
        Identifier for the engine this relation belongs to.
    columns : `~collections.abc.Set`
        Set of columns in the relation.
    unique_keys : `~collections.abc.Set` [ `UniqueKey` ]
        The set of unique constraints this relation is guaranteed to satisfy.
        See `Relation.unique_keys` for details.
    rows : `RowIterable`
        Iterable over mappings that backs this leaf relation.

    Notes
    -----
    `RowIterableLeaf` differs from its subclass, `SerializableRowIterable`, in
    that it does not save its rows when written to a dictionary for
    serialization; instead it is assumed that the name and other state are
    sufficient for a `MappingReader` to construct the iterable.

    Howver, it does reimplement `from_extra_mapping` to construct to
    `SerializableRowIterable` when its serialized form is found, so this method
    can be used by any implementation of `MappingReader` that needs that needs
    to handle either leaf class.
    """

    def __init__(
        self,
        name: str,
        engine: EngineTag,
        columns: Set[_T],
        unique_keys: Set[UniqueKey[_T]],
        rows: RowIterable,
    ):
        super().__init__(
            name,
            engine,
            columns,
            unique_keys=unique_keys,
        )
        self.rows = rows


class SerializableRowIterableLeaf(RowIterableLeaf[_T]):
    """A `Leaf` relation for the native iteration engine, backed by a lazy
    iterable.

    Notes
    -----
    `SerializableRowIterableLeaf` differs from its base class in that it saves
    its rows when written to a dictionary for serialization.  The
    `MappingReader` must be specialized to call
    `SerializeableRowIterableLeaf.from_extra_mapping` appropriately for these
    to be read.
    """

    def write_extra_to_mapping(self) -> Mapping[str, Any]:
        # Docstring inherited.
        columns = list(self.columns)
        columns.sort(key=str)
        return {"rows": [[row[t] for t in columns] for row in self.rows]}

    @classmethod
    def from_extra_mapping(
        cls,
        name: str,
        engine: EngineTag,
        columns: Set[_T],
        unique_keys: Set[UniqueKey[_T]],
        extra: Mapping[str, Any],
    ) -> Leaf[_T]:
        # Docstring inherited.
        columns_sorted = list(columns)
        columns_sorted.sort(key=str)

        def deserialize_rows() -> Iterator[Row[_T]]:
            for list_row in extra["rows"]:
                yield {tag: value for tag, value in zip(columns_sorted, list_row)}

        if unique_keys:
            index_columns = next(iter(unique_keys))
            rows = RowCollection.build_with_unique_index(deserialize_rows(), index_columns)
        else:
            rows = RowCollection(list(deserialize_rows()))
        return cls(name, engine, columns, unique_keys, rows=rows)
