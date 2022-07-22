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

__all__ = ("Engine",)

import operator
from collections.abc import Callable, Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import sqlalchemy

from .._columns import _T
from .._engine import Engine as BaseEngine
from .._exceptions import EngineError
from ._to_executable import ToExecutable
from ._to_logical_column import ToLogicalColumn
from ._to_sql_booleans import ToSqlBooleans

if TYPE_CHECKING:
    from .. import column_expressions
    from .._leaf import Leaf
    from .._relation import Relation
    from ._select_parts import SelectParts


_L = TypeVar("_L")


class Engine(BaseEngine, Generic[_L]):
    """Engine class for converting relation trees to SQLAlchemy executables."""

    def __init__(self, name: str):
        self.name = name
        self.leaf_cache: dict[Leaf, SelectParts] = {}
        self.column_function_cache: dict[str, Callable[..., sqlalchemy.sql.ColumnElement]] = {}

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"lsst.daf.relation.sql.Engine({self.name!r})"

    def evaluate_leaf(self, leaf: Leaf[_T]) -> SelectParts[_T, _L]:
        assert leaf.engine is self, f"Incorrect engine for evaluation: {leaf.engine!r} != {self!r}."
        return self.leaf_cache[leaf]

    def get_column_function(self, name: str) -> Callable[..., sqlalchemy.sql.ColumnElement] | None:
        return self.column_function_cache.get(name, getattr(operator, name, None))

    def to_executable(
        self,
        relation: Relation[_T],
        *,
        distinct: bool = False,
        order_by: Sequence[column_expressions.OrderByTerm[_T]] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> sqlalchemy.sql.expression.SelectBase:
        """Convert a relation tree to an executable SQLAlchemy expression.

        Parameters
        ----------
        relation : `.Relation`
            Root of the relation tree to convert.
        column_types : `ColumnTypeInfo`
            Object that relates column tags to logical columns.
        distinct : `bool`
            Whether to generate an expression whose rows are forced to be
            unique.
        order_by : `Iterable` [ `.column_expressions.OrderByTerm` ]
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
        if relation.engine is not self:
            raise EngineError(
                f"Engine {self!r} cannot operate on relation {relation} with engine {relation.engine!r}."
            )
        return relation.visit(
            ToExecutable(self, distinct=distinct, order_by=order_by, offset=offset, limit=limit)
        )

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

    def convert_expression(
        self, expression: column_expressions.Expression[_T], columns_available: Mapping[_T, _L]
    ) -> _L:
        return expression.visit(ToLogicalColumn(self, columns_available))

    def convert_expression_literal(self, value: Any) -> _L:
        return sqlalchemy.sql.literal(value)

    def convert_predicate(
        self, predicate: column_expressions.Predicate[_T], columns_available: Mapping[_T, _L]
    ) -> sqlalchemy.sql.ColumnElement:
        return predicate.visit(ToSqlBooleans(self, columns_available))

    def convert_order_by(
        self, term: column_expressions.OrderByTerm[_T], columns_available: Mapping[_T, _L]
    ) -> sqlalchemy.sql.ColumnElement:
        # TODO docs
        result = cast(
            sqlalchemy.sql.ColumnElement, self.convert_expression(term.expression, columns_available)
        )
        if term.ascending:
            return result
        else:
            return result.desc()
