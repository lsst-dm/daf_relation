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
    "ExtensionInterface",
    "JoinConditionInterface",
    "OrderByTermInterface",
    "PredicateInterface",
)

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Protocol, TypeVar

import sqlalchemy

from .._columns import _T

if TYPE_CHECKING:
    from .._order_by_term import OrderByTerm
    from ._column_type_info import ColumnTypeInfo
    from ._select_parts import SelectParts

_L = TypeVar("_L")


class ExtensionInterface(Protocol[_T, _L]):
    """Interface for `.Extension` operations in this engine.

    `.Extension` subclasses must either implement this interface or be handled
    by a `ColumnTypeInfo` implementation to be used with the SQL engine.
    """

    def to_sql_executable(
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

    def to_sql_select_parts(self, column_types: ColumnTypeInfo[_T, _L]) -> SelectParts[_T, _L]:
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


class PredicateInterface(Protocol[_T, _L]):
    def to_sql_boolean(
        self, logical_columns: Mapping[_T, _L], column_types: ColumnTypeInfo[_T, _L]
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the predicate.

        Parameters
        ----------
        logical_columns : `Mapping`
            Mapping from `.ColumnTag` to logical column, containing all columns
            available to the predicate.  This will typically involve columns
            beyond those in ``columns_required``.
        column_types : `ColumnTypeInfo`
            Object that relates column tags to logical columns.

        Returns
        -------
        sql_boolean : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression indicating whether this row
            should be included in the relation.
        """
        ...


class OrderByTermInterface(Protocol[_T, _L]):
    def to_sql_sort_column(
        self, logical_columns: Mapping[_T, _L], column_types: ColumnTypeInfo[_T, _L]
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the order-by term.

        Parameters
        ----------
        logical_columns : `Mapping`
            Mapping from `.ColumnTag` to logical column, containing all columns
            available to the order-by-term.  This will typically involve
            columns beyond those in ``columns_required``.
        column_types : `ColumnTypeInfo`
            Object that relates column tags to logical columns.

        Returns
        -------
        sortable : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression to use directly in the
            ORDER BY clause.
        """
        ...


class JoinConditionInterface(Protocol[_T, _L]):
    def to_sql_join_on(
        self,
        logical_columns: tuple[Mapping[_T, _L], Mapping[_T, _L]],
        column_types: ColumnTypeInfo[_T, _L],
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the join condition.

        Parameters
        ----------
        logical_columns : `tuple` [ `Mapping`, `Mapping` ]
            Tuple of mappings from `.ColumnTag` to logical column, containing
            all columns available to the condition from each relation.  This
            will typically involve columns beyond those in
            ``columns_required``.
        column_types : `ColumnTypeInfo`
            Object that relates column tags to logical columns.

        Returns
        -------
        keep : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression indicating whether these rows
            should be included in the joined relation.
        """
        ...
