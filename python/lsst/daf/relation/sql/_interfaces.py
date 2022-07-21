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
    "JoinConditionInterface",
    "OrderByTermInterface",
    "PredicateInterface",
)

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, TypeVar

import sqlalchemy

from .._columns import _T

if TYPE_CHECKING:
    from ._engine import Engine

_L = TypeVar("_L")


class PredicateInterface(Protocol[_T, _L]):
    def to_sql_boolean(
        self, logical_columns: Mapping[_T, _L], engine: Engine[_L]
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the predicate.

        Parameters
        ----------
        logical_columns : `Mapping`
            Mapping from `.ColumnTag` to logical column, containing all columns
            available to the predicate.  This will typically involve columns
            beyond those in ``columns_required``.
        engine : `Engine`
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
        self, logical_columns: Mapping[_T, _L], engine: Engine[_L]
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the order-by term.

        Parameters
        ----------
        logical_columns : `Mapping`
            Mapping from `.ColumnTag` to logical column, containing all columns
            available to the order-by-term.  This will typically involve
            columns beyond those in ``columns_required``.
        engine : `Engine`
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
        engine: Engine[_L],
    ) -> sqlalchemy.sql.ColumnElement:
        """Return a SQLAlchemy expression representing the join condition.

        Parameters
        ----------
        logical_columns : `tuple` [ `Mapping`, `Mapping` ]
            Tuple of mappings from `.ColumnTag` to logical column, containing
            all columns available to the condition from each relation.  This
            will typically involve columns beyond those in
            ``columns_required``.
        engine : `Engine`
            Object that relates column tags to logical columns.

        Returns
        -------
        keep : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy column expression indicating whether these rows
            should be included in the joined relation.
        """
        ...
