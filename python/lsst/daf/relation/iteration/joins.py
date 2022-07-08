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
    "GeneralJoinRowIterable",
    "UniqueIndexJoinRowIterable",
    "make_join_row_iterable",
)

from collections.abc import Iterator, Set
from typing import TYPE_CHECKING, cast

from .._columns import _T, is_unique_key_covered
from .._join_condition import JoinCondition
from ._engine import JoinConditionInterface
from ._row_iterable import RowIterable
from .selection import SelectionRowIterable

if TYPE_CHECKING:
    from .._engines import EngineTag
    from .._relation import Relation
    from .typing import GeneralIndex, Row, UniqueIndex


def make_join_row_iterable(
    base_rows: RowIterable[_T],
    next_rows: RowIterable[_T],
    base_relation: Relation[_T],
    next_relation: Relation[_T],
    conditions: Set[JoinCondition[_T]],
) -> RowIterable[_T]:
    """Return a `RowIterable` that implements a natural join operation.

    Parameters
    ----------
    base_rows : `RowIterable`
        The first iterable being joined.  This iterable's order will be
        preserved and it will always be iterated over lazily (rather than
        copied into an in-memory container), but it is not given an opportunity
        to specialize the join behavior via `RowIterable.try_join`.
    next_rows : `RowIterable`
        The second iterable being joined.  This iterable's order will not
        necessarily be preserved and it may be copied into an in-memory
        container in order to construct an index mapping to perform a
        hash join.  It is given an opportunity to specialize the join behavior
        first via a call to `RowIterable.try_join`.
    base_relation : `.Relation`
        Relation corresponding to ``base_rows``.
    next_relation : `.Relation`
        Relation corresponding to ``next_rows``.
    conditions : `~collections.abc.Set` [ `.JoinCondition` ]
        Special join conditions to apply.  Any not handled by
        `RowIterable.try_join` will be applied as if they were predicates, i.e.
        by filtering rows after they have already been joined on any common
        columns between the rows.

    Returns
    -------
    join_rows : `RowIterable`
        New iterable that implements the join operation.
    """
    flipped_conditions = JoinCondition.find_matching(base_relation.columns, next_relation.columns, conditions)
    assert flipped_conditions == conditions, "Expect same contents, but maybe some flipped."
    join_rows, matched_conditions = next_rows.try_join(
        next_relation, base_rows, base_relation, flipped_conditions
    )
    if join_rows is not None:
        return _finish_join_row_iterable(base_relation.engine.tag, join_rows, conditions - matched_conditions)
    common_columns = frozenset(base_relation.columns & next_relation.columns)
    if is_unique_key_covered(common_columns, next_relation.unique_keys):
        next_rows_with_unique_index = next_rows.with_unique_index(common_columns)
        return _finish_join_row_iterable(
            base_relation.engine.tag,
            UniqueIndexJoinRowIterable(
                base_rows,
                next_rows_with_unique_index.get_unique_index(common_columns),
                on_key=common_columns,
            ),
            conditions,
        )
    else:
        next_rows_with_general_index = next_rows.with_general_index(common_columns)
        return _finish_join_row_iterable(
            base_relation.engine.tag,
            GeneralJoinRowIterable(
                base_rows,
                next_rows_with_general_index.get_general_index(common_columns),
                on_key=common_columns,
            ),
            conditions,
        )


def _finish_join_row_iterable(
    engine: EngineTag,
    base: RowIterable[_T],
    missing_conditions: Set[JoinCondition[_T]],
) -> RowIterable[_T]:
    """Helper function that handles any missing join conditions by applying
    them as predicates.

    Parameters
    ----------
    engine : `.EngineTag`
        Engine tag for the join.  This must always be the native iteration
        engine, but it's easier to pass it into this (private) function from
        the relation than import the singleton directly, since that would
        involve import cycles.
    base : `RowIterable`
        Row iterable that implements the join, but does not necessarily utilize
        all matching join conditions.
    missing_conditions : `~collections.abc.Set` [ `.JoinCondition` ]
        `.JoinCondition` objects that match this join but have not been
        applied.

    Returns
    -------
    join_rows : `RowIterable`
        Row iterable that fully implements the join, including all join
        conditions.
    """
    if not missing_conditions:
        return base
    return SelectionRowIterable(base, tuple(cast(JoinConditionInterface, c) for c in missing_conditions))


class UniqueIndexJoinRowIterable(RowIterable[_T]):
    """A `RowIterable` that implements a join when one operand is unique over
    the join's common columns.

    Parameters
    ----------
    base : `RowIterable`
        Row iterable for the first operand in a join.
    next_index : `UniqueIndex`
        Mapping with `tuple` keys and `Row` values.
    on_key : `frozenset` [ `.ColumnTag` ]
        Columns present in both iterables that they should be joined on.  Must
        be ordered the same as the keys of ``next_index``.
    """

    def __init__(self, base: RowIterable[_T], next_index: UniqueIndex[_T], on_key: frozenset[_T]):
        self.base = base
        self.next_index = next_index
        self.on_key = on_key

    def __iter__(self) -> Iterator[Row[_T]]:
        for base_row in self.base:
            key = tuple(base_row[k] for k in self.on_key)
            if (next_row := self.next_index.get(key)) is not None:
                yield {**base_row, **next_row}


class GeneralJoinRowIterable(RowIterable[_T]):
    """A `RowIterable` that implements a join when neither operand is unique
    over the join's common columns.

    Parameters
    ----------
    base : `RowIterable`
        Row iterable for the first operand in a join.
    next_index : `GeneralIndex`
        Mapping with `tuple` keys and `Sequence` of `Row` values.
    on_key : `frozenset` [ `.ColumnTag` ]
        Columns present in both iterables that they should be joined on.  Must
        be ordered the same as the keys of ``next_index``.
    """

    def __init__(self, base: RowIterable[_T], next_index: GeneralIndex[_T], on_key: frozenset[_T]):
        self.base = base
        self.next_index = next_index
        self.on_key = on_key

    def __iter__(self) -> Iterator[Row[_T]]:
        for base_row in self.base:
            key = tuple(base_row[k] for k in self.on_key)
            for next_row in self.next_index.get(key, ()):
                yield {**base_row, **next_row}
