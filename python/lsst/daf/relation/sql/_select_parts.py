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

__all__ = ("MutableSelectParts", "SelectParts", "SelectPartsLeaf", "ToSelectParts")

import dataclasses
from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any, Generic, cast

import sqlalchemy

from .. import operations
from .._columns import _T, UniqueKey
from .._exceptions import EngineError
from .._leaf import Leaf
from .._relation_visitor import RelationVisitor
from ._column_type_info import _L, ColumnTypeInfo
from ._interfaces import JoinConditionInterface, OrderByTermInterface, PredicateInterface

if TYPE_CHECKING:
    from .._join_condition import JoinCondition
    from .._order_by_term import OrderByTerm
    from .._relation import Identity, Relation, Zero
    from .._serialization import DictWriter
    from ._engine import Engine


@dataclasses.dataclass(slots=True, eq=False)
class SelectParts(Generic[_T, _L]):
    """A struct that represents a simple SELECT query's parts."""

    from_clause: sqlalchemy.sql.FromClause
    """SQLAlchemy representation of the FROM clause
    (`sqlalchemy.sql.FromClause`).
    """

    where: Sequence[sqlalchemy.sql.ColumnElement]
    """SQLAlchemy representation of the WHERE clause, as a sequence of
    boolean expressions to be combined with ``AND``
    (`Sequence` [ `sqlalchemy.sql.ColumnElement` ]).
    """

    columns_available: Mapping[_T, _L] | None
    """Mapping from `.ColumnTag` to logical column for the columns available
    from the FROM clause (`Mapping` or `None`).

    If `None`, the columns available are just the columns provided by the
    relation these parts represent, and they can be obtained as needed by
    calling `ColumnTypeInfo.extract_mapping` on `from_clause`.  This is an
    optimization that avoids calls to `ColumnTypeInfo.extract_mapping` when
    `columns_available` isn't actually needed.
    """

    def to_executable(
        self,
        relation: Relation[_T],
        column_types: ColumnTypeInfo[_T, _L],
        *,
        distinct: bool = False,
        order_by: Sequence[OrderByTerm[_T]] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> sqlalchemy.sql.Select:
        """Create a SQL executable from this struct.

        Parameters
        ----------
        relation : `.Relation`
            Relation this struct was built from.
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
        select : `sqlalchemy.sql.Select`
            SQL SELECT statement.
        """
        select_parts = relation.visit(ToSelectParts(column_types))
        if select_parts.columns_available is None:
            columns_available: Mapping[_T, _L] = column_types.extract_mapping(
                relation.columns, select_parts.from_clause.columns
            )
            columns_projected = columns_available
        else:
            columns_available = select_parts.columns_available
            columns_projected = {tag: columns_available[tag] for tag in relation.columns}
        select = column_types.select_items(columns_projected.items(), select_parts.from_clause)
        if len(select_parts.where) == 1:
            select = select.where(select_parts.where[0])
        elif select_parts.where:
            select = select.where(sqlalchemy.sql.and_(*select_parts.where))
        if distinct:
            select = select.distinct()
        if order_by:
            select = select.order_by(
                *[
                    cast(OrderByTermInterface[_T, _L], o).to_sql_sort_column(columns_available, column_types)
                    for o in order_by
                ]
            )
        if offset:
            select = select.offset(offset)
        if limit is not None:
            select = select.limit(limit)
        return select


@dataclasses.dataclass(slots=True, eq=False)
class MutableSelectParts(SelectParts[_T, _L]):
    """A mutable variant of `SelectParts.

    This is intended primarily for construction of `SelectPartsLeaf` instances,
    where it's convenient to be able to modify the struct in place before
    passing it to the leaf constructor, and `columns_available` is not allowed
    to be `None`.
    """

    where: list[sqlalchemy.sql.ColumnElement] = dataclasses.field(default_factory=list)
    columns_available: dict[_T, _L] = dataclasses.field(default_factory=dict)


class SelectPartsLeaf(Leaf[_T], Generic[_T, _L]):
    """The leaf relation type for the SQL engine.

    Parameters
    ----------
    engine : `Engine`
        Identifier for the engine this relation belongs to.
    select_parts : `SelectParts`
        The `SelectParts` struct that backs this relation.
    columns : `~collections.abc.Set`. optional
        Set of columns in the relation.  If not provided,
        ``select_parts.columns_available`` is used and must not be `None`.
    unique_keys : `~collections.abc.Set` [ `UniqueKey` ], optional
        The set of unique constraints this relation is guaranteed to satisfy.
        See `Relation.unique_keys` for details.
    extra : `Mapping`, optional
        Extra information to serialize with this relation.

    Notes
    -----
    This class never attempts to serialize its `SelectParts` state, and cannot
    be fully deserialized without a custom implementation of `.MappingReader`
    that knows how to construct a `SelectParts` instance from ``extra``.
    """

    def __init__(
        self,
        engine: Engine,
        select_parts: SelectParts[_T, _L],
        *,
        columns: Set[_T] | None = None,
        unique_keys: Set[UniqueKey[_T]] = frozenset(),
        extra: Mapping[str, Any] | None = None,
    ):
        if columns is None:
            if select_parts.columns_available is None:
                raise RuntimeError(
                    "SelectParts used to construct leaf must have explicit "
                    "columns_available unless columns is provided."
                )
            else:
                columns = select_parts.columns_available.keys()
        super().__init__(engine, columns, unique_keys)
        self.select_parts = select_parts
        self.extra = extra if extra is not None else {}

    def serialize(self, writer: DictWriter) -> dict[str, Any]:
        # Docstring inherited.
        result = super().serialize(writer)
        result.update(self.extra)
        return result


@dataclasses.dataclass(eq=False, slots=True)
class ToSelectParts(RelationVisitor[_T, SelectParts[_T, _L]], Generic[_T, _L]):
    """A `.RelationVisitor` implemention that converts a `.Relation` tree into
    a `SelectParts` struct.

    This visitor directly handles `.Leaf`, `.Extension`, `.operations.Join`,
    `.operations.Projection`, and `.operations.Selection` relations, and
    delegates the others to `ToExecutable`.  It does not handle transfers at
    all.
    """

    column_types: ColumnTypeInfo[_T, _L]
    """Object that relates column tags to logical columns for this visitor
    (`ColumnTypeInfo`).
    """

    def visit_distinct(self, visited: operations.Distinct[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        return SelectParts(
            self._use_executable(visited),
            [],
            None,
        )

    def visit_identity(self, visited: Identity[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        return SelectParts(
            self.column_types.make_identity_subquery(),
            [],
            None,
        )

    def visit_leaf(self, visited: Leaf[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        return cast(SelectPartsLeaf[_T, _L], visited).select_parts

    def visit_join(self, visited: operations.Join[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        lhs_parts = visited.lhs.visit(self)
        if lhs_parts.columns_available is None:
            lhs_parts.columns_available = self.column_types.extract_mapping(
                visited.lhs.columns, lhs_parts.from_clause.columns
            )
        rhs_parts = visited.rhs.visit(self)
        if rhs_parts.columns_available is None:
            rhs_parts.columns_available = self.column_types.extract_mapping(
                visited.rhs.columns, rhs_parts.from_clause.columns
            )
        on_terms: list[sqlalchemy.sql.ColumnElement] = []
        for tag in lhs_parts.columns_available.keys() & rhs_parts.columns_available.keys():
            on_terms.append(lhs_parts.columns_available[tag] == rhs_parts.columns_available[tag])
        if visited.condition is not None:
            on_terms.append(
                cast(JoinConditionInterface, visited.condition).to_sql_join_on(
                    (lhs_parts.columns_available, rhs_parts.columns_available), self.column_types
                )
            )
        on_clause: sqlalchemy.sql.ColumnElement
        if not on_terms:
            on_clause = sqlalchemy.sql.literal(True)
        elif len(on_terms) == 1:
            on_clause = on_terms[0]
        else:
            on_clause = sqlalchemy.sql.and_(*on_terms)
        return SelectParts(
            from_clause=lhs_parts.from_clause.join(rhs_parts.from_clause, onclause=on_clause),
            where=tuple(lhs_parts.where) + tuple(rhs_parts.where),
            columns_available={**lhs_parts.columns_available, **rhs_parts.columns_available},
        )

    def visit_zero(self, visited: Zero[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        return SelectParts(
            self._use_executable(visited),
            [],
            None,
        )

    def visit_projection(self, visited: operations.Projection[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        # We can just delegate to base because projection only affects
        # to_executable, and the default implementation for that already only
        # selects the relation's own columns.
        return visited.base.visit(self)

    def visit_selection(self, visited: operations.Selection[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        base_parts = visited.base.visit(self)
        if base_parts.columns_available is None:
            base_parts.columns_available = self.column_types.extract_mapping(
                visited.base.columns, base_parts.from_clause
            )
        new_where = cast(PredicateInterface, visited.predicate).to_sql_boolean(
            base_parts.columns_available, self.column_types
        )
        return dataclasses.replace(
            base_parts,
            where=tuple(base_parts.where) + (new_where,),
        )

    def visit_slice(self, visited: operations.Slice[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        return SelectParts(
            self._use_executable(visited),
            [],
            None,
        )

    def visit_transfer(self, visited: operations.Transfer) -> SelectParts[_T, _L]:
        # Docstring inherited.
        raise EngineError("SQL conversion only works on relation trees with no transfers.")

    def visit_union(self, visited: operations.Union[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        return SelectParts(
            self._use_executable(visited),
            [],
            None,
        )

    def _use_executable(self, relation: Relation[_T]) -> sqlalchemy.sql.FromClause:
        """Delegate to `ToExecutable` to implement visitation for a relation.

        Parameters
        ----------
        relation : `.Relation`
            Relation to process.

        Returns
        -------
        select : `sqlalchemy.sql.FromClause`
            SQL FROM clause.
        """
        from ._to_executable import ToExecutable

        return relation.visit(ToExecutable(self.column_types)).subquery()

    def _join_select_parts(
        self,
        base_parts: SelectParts[_T, _L],
        next_relation: Relation[_T],
        conditions: Iterable[JoinCondition[_T]],
    ) -> SelectParts[_T, _L]:
        """Join two relations via their `SelectParts` representation.

        Parameters
        ----------
        base_parts : `SelectParts`
            Simple SELECT statement parts for the first operand.  Must not have
            `SelectParts.columns_available` set to `None`.
        next_relation : `.Relation`
            `.Relation` for the other operand.
        conditions : `Iterable` [ `.JoinCondition` ]
            Join conditions that match this paticular join.

        Returns
        -------
        join_parts : `SelectParts`
            Simple SELECT statementparts representing the join.
        """
        assert base_parts.columns_available is not None
        next_parts = next_relation.visit(self)
        if next_parts.columns_available is None:
            next_parts.columns_available = self.column_types.extract_mapping(
                next_relation.columns, next_parts.from_clause.columns
            )
        on_terms: list[sqlalchemy.sql.ColumnElement] = []
        for tag in base_parts.columns_available.keys() & next_parts.columns_available.keys():
            on_terms.append(base_parts.columns_available[tag] == next_parts.columns_available[tag])
        for condition in conditions:
            on_terms.append(
                cast(JoinConditionInterface, condition).to_sql_join_on(
                    (base_parts.columns_available, next_parts.columns_available), self.column_types
                )
            )
        on_clause: sqlalchemy.sql.ColumnElement
        if not on_terms:
            on_clause = sqlalchemy.sql.literal(True)
        elif len(on_terms) == 1:
            on_clause = on_terms[0]
        else:
            on_clause = sqlalchemy.sql.and_(*on_terms)
        from_clause = base_parts.from_clause.join(next_parts.from_clause, onclause=on_clause)
        where = tuple(base_parts.where) + tuple(next_parts.where)
        columns_available = {**base_parts.columns_available, **next_parts.columns_available}
        return SelectParts(from_clause, where, columns_available)
