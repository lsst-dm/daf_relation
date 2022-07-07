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
    from .._extension import Extension
    from .._join_condition import JoinCondition
    from .._order_by_term import OrderByTerm
    from .._predicate import Predicate
    from ._select_parts import SelectParts

_L = TypeVar("_L")

_L_con = TypeVar("_L_con", contravariant=True)


class ExtensionInterface(Protocol[_T, _L]):
    """Interface for `.Extension` operations in this engine.

    `.Extension` subclasses must either implement this interface or be handled
    by a `ColumnTypeInfo` implementation to be used with the SQL engine.
    """

    def to_executable(
        self,
        column_types: ColumnTypeInfo[_T, _L],
        *,
        distinct: bool = False,
        order_by: Sequence[OrderByTerm[_T]] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> sqlalchemy.sql.expression.SelectBase:
        """Convert this relation into SQL as an executable query.

        Parameters
        ----------
        column_types : `ColumnTypeInfo`
            Object that relates column tags to logical columns.
        distinct : `bool`
            Whether to generate an expression whose rows are forced to be
            unique.
        order_by : `Iterable` [ `.OrderByTerm` ]
            Iterable of objects that specify a sort order.
        offset : `int`, optional
            Starting index for returned rows, with ``0`` as the first row.
        limit : `int` or `None`, optional
            Maximum number of rows returned, or `None` (default) for no limit.

        Returns
        -------
        select : `sqlalchemy.sql.expression.SelectBase`
            A SQLAlchemy SELECT or compound SELECT query.
        """
        ...

    def to_select_parts(self, column_types: ColumnTypeInfo[_T, _L]) -> SelectParts[_T, _L]:
        """Convert this relation into SQL as a `SelectParts` struct.

        Parameters
        ----------
        column_types : `ColumnTypeInfo`
            Object that relates column tags to logical columns.

        Returns
        -------
        select_parts : `SelectParts`
            Struct representing a simple SELECT query.
        """
        ...


class PredicateState(Protocol[_T, _L_con]):
    """Callable protocol for the values of `.Predicate.engine_state` for this
    engine.
    """

    def __call__(self, logical_columns: Mapping[_T, _L_con], /) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the predicate.

        Parameters
        ----------
        logical_columns : `Mapping`
            Mapping from `.ColumnTag` to logical column, containing all columns
            available to the predicate.  This will typically involve columns
            beyond those in ``columns_required``.

        Returns
        -------
        keep : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression indicating whether this row
            should be included in the relation.
        """
        ...


class OrderByTermState(Protocol[_T, _L_con]):
    """Callable protocol for the values of `.OrderByTerm.engine_state` for this
    engine.
    """

    def __call__(
        self, logical_columns: Mapping[_T, _L_con], /, ascending: bool
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the order-by term.

        Parameters
        ----------
        logical_columns : `Mapping`
            Mapping from `.ColumnTag` to logical column, containing all columns
            available to the order-by-term.  This will typically involve
            columns beyond those in ``columns_required``.
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
    """Callable protocol for the values of `.JoinCondition.engine_state` for
    this engine.
    """

    def __call__(
        self,
        logical_columns: tuple[Mapping[_T, _L_con], Mapping[_T, _L_con]],
        /,
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

    def extract_mapping(self, tags: Set[_T], sql_columns: sqlalchemy.sql.ColumnCollection) -> dict[_T, _L]:
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
        logical_columns : `dict`
            Dictionary mapping `.ColumnTag` to logical column type.
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
    ) -> sqlalchemy.sql.ColumnElement:
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
            return callable(logical_columns, condition.was_flipped)
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
            return callable(logical_columns, order_by.ascending)
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
    ) -> sqlalchemy.sql.ColumnElement:
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
            return callable(logical_columns)
        else:
            raise NotImplementedError(
                f"No default SQL implementation for Predicate {predicate} "
                f"with no state for engine {engine}."
            )

    def convert_extension_to_executable(
        self,
        extension: Extension[_T],
        *,
        distinct: bool = False,
        order_by: Sequence[OrderByTerm[_T]] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> sqlalchemy.sql.expression.SelectBase:
        """Convert a relation into SQL as an executable query.

        Parameters
        ----------
        extension : `Extension`
            Extension relation to convert.
        distinct : `bool`
            Whether to generate an expression whose rows are forced to be
            unique.
        order_by : `Iterable` [ `.OrderByTerm` ]
            Iterable of objects that specify a sort order.
        offset : `int`, optional
            Starting index for returned rows, with ``0`` as the first row.
        limit : `int` or `None`, optional
            Maximum number of rows returned, or `None` (default) for no limit.

        Returns
        -------
        select : `sqlalchemy.sql.expression.SelectBase`
            A SQLAlchemy SELECT or compound SELECT query.

        Notes
        -----
        The default implementation assumes ``extension`` satisfies
        `ExtensionInterface` and delegates to
        `~ExtensionInterface.to_executable`.
        """
        sql_extension = cast(ExtensionInterface, extension)
        return sql_extension.to_executable(
            self, distinct=distinct, order_by=order_by, offset=offset, limit=limit
        )

    def convert_extension_to_select_parts(self, extension: Extension[_T]) -> SelectParts[_T, _L]:
        """Convert a relation into SQL as a `SelectParts` struct.

        Parameters
        ----------
        extension : `Extension`
            Extension relation to convert.

        Returns
        -------
        select_parts : `SelectParts`
            Struct representing a simple SELECT query.

        Notes
        -----
        The default implementation assumes ``extension`` satisfies
        `ExtensionInterface` and delegates to
        `~ExtensionInterface.to_select_parts`.
        """
        sql_extension = cast(ExtensionInterface, extension)
        return sql_extension.to_select_parts(self)
