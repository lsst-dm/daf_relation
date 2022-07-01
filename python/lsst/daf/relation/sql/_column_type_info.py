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
    "ColumnTypeInfo",
    "JoinConditionState",
    "OrderByTermState",
    "PredicateState",
)

from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar, cast

import sqlalchemy

from .._columns import _T

if TYPE_CHECKING:
    from .._engines import EngineTag
    from .._join_condition import JoinCondition
    from .._order_by_term import OrderByTerm
    from .._predicate import Predicate


_L = TypeVar("_L")

_L_con = TypeVar("_L_con", contravariant=True)


class PredicateState(Protocol[_T, _L_con]):
    """Callable protocol for the values of `Predicate.engine_state` for this
    engine.
    """

    def __call__(
        self, logical_columns: Mapping[_T, _L_con], columns_required: Set[_T]
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the predicate.

        Parameters
        ----------
        logical_columns : `Mapping`
            Mapping from `.ColumnTag` to logical column, containing all columns
            available to the predicate.  This will typically involve columns
            beyond those in ``columns_required``.
        columns_required : `~collections.abc.Set`
            Forwarded from `Predicate.columns_required`.

        Returns
        -------
        keep : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression indicating whether this row
            should be included in the relation.
        """
        ...


class OrderByTermState(Protocol[_T, _L_con]):
    """Callable protocol for the values of `OrderByTerm.engine_state` for this
    engine.
    """

    def __call__(
        self, logical_columns: Mapping[_T, _L_con], columns_required: Set[_T], ascending: bool
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the order-by term.

        Parameters
        ----------
        logical_columns : `Mapping`
            Mapping from `.ColumnTag` to logical column, containing all columns
            available to the order-by-term.  This will typically involve
            columns beyond those in ``columns_required``.
        columns_required : `~collections.abc.Set`
            Forwarded from `OrderByTerm.columns_required`.
        ascending : `bool`
            Whether to sort ascending (`True`) or descending (`False`).

        Returns
        -------
        sortable : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression to use directly in the
            ORDER BY clause.
        """
        ...


class JoinConditionState(Protocol[_T, _L_con]):
    """Callable protocol for the values of `JoinCondition.engine_state` for
    this engine.
    """

    def __call__(
        self,
        logical_columns: tuple[Mapping[_T, _L_con], Mapping[_T, _L_con]],
        columns_required: tuple[Set[_T], Set[_T]],
        was_flipped: bool,
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the join condition.

        Parameters
        ----------
        logical_columns : `tuple` [ `Mapping`, `Mapping` ]
            Tuple of mappings from `.ColumnTag` to logical column, containing
            all columns available to the condition from each relation.  This
            will typically involve columns beyond those in
            ``columns_required``.
        columns_required : `~collections.abc.Set`
            Forwarded from `JoinCondnition.columns_required`.  Is ordered
            consistently with ``logical_columns``.
        was_flipped : `bool`
            Whether this join condition was flipped relative to its original
            definition.

        Returns
        -------
        keep : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression indicating whether these rows
            should be included in the joined relation.
        """
        ...


class ColumnTypeInfo(Generic[_T, _L]):
    """A helper object that provides information about column types to the SQL
    engine.

    ColumnTypeInfo is generic, with the first parameter a `.ColumnTag` class
    and the second what we call the "logical column" type: the Python type used
    to represent columns in a SQL query.  This can be any type as long as it is
    used consistently by all `ColumnTypeInfo` methods.

    Notes
    -----
    While `ColumnTypeInfo` is a concrete class that may be used as-is (if the
    "logical column" type is just `sqlalchemy.sql.ColumnElement`), it exists
    primarily as a customization hook for extensions that may want to use the
    engine with other logical column types.

    While reimplementations can change this, too (in particular by
    reimplementing `select_items`), `ColumnTypeInfo` uses the `str`
    representation of the `.ColumnTag` for the names of columns in SQLAlchemy,
    and hence those strings are the keys that should be used to access those
    columns on result rows when a query is actually executed.
    """

    def extract_mapping(self, tags: Set[_T], sql_columns: sqlalchemy.sql.ColumnCollection) -> Mapping[_T, _L]:
        """Extract a mapping with `.ColumnTag` keys and logical column values
        from a SQLAlchemy column collection.

        Parameters
        ----------
        tags : `~collections.abc.Set`
            Set of `.ColumnTag` objects whose logical columns should be
            extracted.
        sql_columns : `sqlalchemy.sql.ColumnCollection`
            SQLAlchemy collection of columns, such as
            `sqlalchemy.sql.FromClause.columns`.

        Returns
        -------
        logical_columns : `Mapping`
            Mapping from `.ColumnTag` to logical column type.
        """
        return {tag: cast(_L, sql_columns[str(tag)]) for tag in tags}

    def select_items(
        self, items: Iterable[tuple[_T, _L]], sql_from: sqlalchemy.sql.FromClause
    ) -> sqlalchemy.sql.Select:
        """Construct a SQLAlchemy representation of a SELECT query.

        Parameters
        ----------
        items : `Iterable` [ `tuple` ]
            Iterable of (`.ColumnTag`, logical column) pairs.  This is
            typically the ``items()`` of a mapping returned by
            `extract_mapping` or obtained from `SelectParts.columns_available`.
        sql_from : `sqlalchemy.sql.FromClause`
            SQLAlchemy representation of a FROM clause, such as a single table,
            aliased subquery, or join expression.  Must provide all columns
            referenced by ``items``.

        Returns
        -------
        select : `sqlalchemy.sql.Select`
            SELECT query.

        Notes
        -----
        This method is responsible for handling the case where ``items`` is
        empty, typically by delegating to `handle_empty_columns`.
        """
        select_columns = [
            cast(sqlalchemy.sql.ColumnElement, logical_column).label(str(tag))
            for tag, logical_column in items
        ]
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).select_from(sql_from)

    def make_zero_select(self, tags: Set[_T]) -> sqlalchemy.sql.Select:
        """Construct a SQLAlchemy SELECT query that yields no rows.

        Parameters
        ----------
        tags : `~collections.abc.Set`
            Set of tags for the columns the query should have.

        Returns
        -------
        zero_select : `sqlalchemy.sql.Select`
            SELECT query that yields no rows.

        Notes
        -----
        This method is responsible for handling the case where ``items`` is
        empty, typically by delegating to `handle_empty_columns`.
        """
        select_columns = [sqlalchemy.sql.literal(None).label(str(tag)) for tag in tags]
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).where(sqlalchemy.sql.literal(False))

    def make_unit_subquery(self) -> sqlalchemy.sql.FromClause:
        """Construct a SQLAlchemy FROM clause with one row and no (meaningful)
        columns.

        Returns
        -------
        unit_from : `sqlalchemy.sql.FromClause`
            FROM clause with one column and no meaningful columns.

        Notes
        -----
        SQL SELECT queries and similar queries are not permitted to actually
        have no columns, but we can add a literal column that isn't associated
        with any `.ColumnTag`, making it appear to the relation system as if
        there are no columns.  The default implementation does this by
        delegating to `handle_empty_columns`.
        """
        select_columns: list[sqlalchemy.sql.ColumnElement] = []
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).subquery()

    def handle_empty_columns(self, columns: list[sqlalchemy.sql.ColumnElement]) -> None:
        """Handle the edge case where a SELECT statement has no columns, by
        adding a literal column that should be ignored.

        Parameters
        ----------
        columns : `list` [ `sqlalchemy.sql.ColumnElement` ]
            List of SQLAlchemy column objects.  This may have no elements when
            this method is called, and must always have at least one element
            when it returns.
        """
        if not columns:
            columns.append(sqlalchemy.sql.literal(True).label("IGNORED"))

    def convert_join_condition(
        self,
        engine: EngineTag,
        condition: JoinCondition[_T],
        logical_columns: tuple[Mapping[_T, _L], Mapping[_T, _L]],
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        """Convert a `.JoinCondition` instance into a SQLAlchemy expression.

        Parameters
        ----------
        engine : `.EngineTag`
            Identifier for the engine the relation belongs to.
        condition : `.JoinCondition`
            Join condition to convert.
        logical_columns : `tuple` [ `Mapping`, `Mapping` ]
            Pair of mapping from column tag to logical column, with all columns
            available to the join condition from either relation.

        Returns
        -------
        keep : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression to use in a JOIN ON
            expression.

        Notes
        -----
        The default implementation assumes that
        ``condition.engine_state[engine]`` is a callable with the
        `JoinTermState` signature.
        """
        callable: JoinConditionState[_T, _L] | None = condition.engine_state.get(engine)
        if callable is not None:
            return callable(logical_columns, condition.columns_required, condition.was_flipped)
        else:
            raise NotImplementedError(
                f"No default SQL implementation for Predicate {condition} "
                f"with no state for engine {engine}."
            )

    def convert_order_by_term(
        self, engine: EngineTag, order_by: OrderByTerm[_T], logical_columns: Mapping[_T, _L]
    ) -> sqlalchemy.sql.ColumnElement:
        """Convert an `.OrderByTerm` instance into a SQLAlchemy expression.

        Parameters
        ----------
        engine : `.EngineTag`
            Identifier for the engine the relation belongs to.
        order_by : `.OrderByTerm`
            Order-by term to convert.
        logical_columns : `Mapping`
            Mapping from column tag to logical column, with all columns
            available to the order-by term.

        Returns
        -------
        sortable : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression to use directly in the
            ORDER BY clause.

        Notes
        -----
        The default implementation assumes that
        ``order_by.engine_state[engine]`` is either:

        - a callable with the `OrderByTermState` signature;
        - `None`, but ``len(order_by.columns_required) == 1``, and the intent
          is to sort by that column directly.
        """
        callable: OrderByTermState[_T, _L] | None = order_by.engine_state.get(engine)
        if callable is not None:
            return callable(logical_columns, order_by.columns_required, order_by.ascending)
        elif len(order_by.columns_required) == 1:
            (tag,) = order_by.columns_required
            logical_column = cast(sqlalchemy.sql.ColumnElement, logical_columns[tag])
            return logical_column if order_by.ascending else logical_column.desc()
        else:
            raise NotImplementedError(
                f"No default SQL implementation for OrderByTerm {order_by} "
                f"with no state for engine {engine} and multiple columns required."
            )

    def convert_predicate(
        self, engine: EngineTag, predicate: Predicate[_T], logical_columns: Mapping[_T, _L]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        """Convert a `.Predicate` instance into a SQLAlchemy expression.

        Parameters
        ----------
        engine : `.EngineTag`
            Identifier for the engine the relation belongs to.
        predicate : `.Predicate`
            Predicate to convert.
        logical_columns : `tuple` [ `Mapping`, `Mapping` ]
            Mapping from column tag to logical column, with all columns
            available to the predicate.

        Returns
        -------
        keep : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression to use in a WHERE
            expression.

        Notes
        -----
        The default implementation assumes that
        ``predicate.engine_state[engine]`` is a callable with the
        `PredicateState` signature.
        """
        callable: PredicateState[_T, _L] | None = predicate.engine_state.get(engine)
        if callable is not None:
            return callable(logical_columns, predicate.columns_required)
        else:
            raise NotImplementedError(
                f"No default SQL implementation for Predicate {predicate} "
                f"with no state for engine {engine}."
            )
