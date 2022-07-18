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

__all__ = ("ColumnTypeInfo",)

from collections.abc import Iterable, Set
from typing import Generic, TypeVar, cast

import sqlalchemy

from .._columns import _T

_L = TypeVar("_L")


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
        self,
        items: Iterable[tuple[_T, _L]],
        sql_from: sqlalchemy.sql.FromClause,
        *extra: sqlalchemy.sql.ColumnElement,
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
        *extra : `sqlalchemy.sql.ColumnElement`
            Additional SQL column expressions to include.

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
        select_columns.extend(extra)
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

    def make_identity_subquery(self) -> sqlalchemy.sql.FromClause:
        """Construct a SQLAlchemy FROM clause with one row and no (meaningful)
        columns.

        Returns
        -------
        identity_from : `sqlalchemy.sql.FromClause`
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
