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

from collections.abc import Iterator
from typing import TYPE_CHECKING

from .._columns import _T, is_unique_key_covered
from ._row_iterable import RowIterable
from .selection import SelectionRowIterable

if TYPE_CHECKING:
    from .. import column_expressions
    from .._relation import Relation
    from ._engine import Engine
    from .typing import GeneralIndex, Row, UniqueIndex


def make_join_row_iterable(
    engine: Engine[_T],
    lhs_rows: RowIterable[_T],
    rhs_rows: RowIterable[_T],
    lhs_relation: Relation[_T],
    rhs_relation: Relation[_T],
    condition: column_expressions.JoinCondition[_T],
) -> RowIterable[_T]:
    """Return a `RowIterable` that implements a natural join operation.

    Parameters
    ----------
    engine : `Engine`
        Iteration engine.
    lhs_rows : `RowIterable`
        The first iterable being joined.  This iterable's order will be
        preserved and it will always be iterated over lazily (rather than
        copied into an in-memory container), but it is not given an opportunity
        to specialize the join behavior via `RowIterable.try_join`.
    rhs_rows : `RowIterable`
        The second iterable being joined.  This iterable's order will not
        necessarily be preserved and it may be copied into an in-memory
        container in order to construct an index mapping to perform a hash
        join.  It is given an opportunity to specialize the join behavior first
        via a call to `RowIterable.try_join`.
    lhs_relation : `.Relation`
        Relation corresponding to ``lhs_rows``.
    rhs_relation : `.Relation`
        Relation corresponding to ``rhs_rows``.
    condition : `.column_expressions.JoinCondition` or None
        Explicit condition that must be satisfied by returned join rows,
        including automatic common columns equality constraints and an optional
        custom predicate.

    Returns
    -------
    join_rows : `RowIterable`
        New iterable that implements the join operation.
    """
    join_rows, was_predicate_applied = rhs_rows.try_join(rhs_relation, lhs_rows, lhs_relation, condition)
    if join_rows is not None:
        return _finish_join_row_iterable(
            engine, join_rows, None if was_predicate_applied else condition.predicate
        )
    common_columns = frozenset(condition.common_columns)
    if is_unique_key_covered(common_columns, rhs_relation.unique_keys):
        rhs_rows_with_unique_index = rhs_rows.with_unique_index(common_columns)
        return _finish_join_row_iterable(
            engine,
            UniqueIndexJoinRowIterable(
                lhs_rows,
                rhs_rows_with_unique_index.get_unique_index(common_columns),
                on_key=common_columns,
            ),
            condition.predicate,
        )
    else:
        rhs_rows_with_general_index = rhs_rows.with_general_index(common_columns)
        return _finish_join_row_iterable(
            engine,
            GeneralJoinRowIterable(
                lhs_rows,
                rhs_rows_with_general_index.get_general_index(common_columns),
                on_key=common_columns,
            ),
            condition.predicate,
        )


def _finish_join_row_iterable(
    engine: Engine[_T],
    base: RowIterable[_T],
    predicate: column_expressions.Predicate[_T] | None,
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
    predicate : `column_expressions.Predicate`, optional
        Explicit condition that must be satisfied by returned join rows, in
        addition to automatic equality constraints on common columns already
        applied.

    Returns
    -------
    join_rows : `RowIterable`
        Row iterable that fully implements the join, including all join
        conditions.
    """
    from ._iteration_visitor import IterationVisitor

    if predicate is None:
        return base
    return SelectionRowIterable(base, predicate.visit(IterationVisitor(engine)))


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
