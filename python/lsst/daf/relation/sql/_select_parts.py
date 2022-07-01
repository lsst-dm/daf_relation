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

__all__ = ("SelectParts", "SelectPartsLeaf", "ToSelectParts")

import dataclasses
from collections import deque
from collections.abc import Iterable, Iterator, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any, Generic, cast

import sqlalchemy

from .. import operations
from .._columns import _T
from .._exceptions import EngineError
from .._leaf import Leaf
from .._relation_visitor import RelationVisitor
from ._column_type_info import _L, ColumnTypeInfo

if TYPE_CHECKING:
    from .._join_condition import JoinCondition
    from .._relation import Relation


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


class SelectPartsLeaf(Leaf[_T], Generic[_T, _L]):
    """The leaf relation type for the SQL engine.

    Parameters
    ----------
    *args
        Positional arguments forwarded to the `Leaf` constructor.
    select_parts : `SelectParts`
        The `SelectParts` struct that backs this relation.
    extra : `Mapping`
        Extra information to serialize with this relation.

    Notes
    -----
    This class never attempts to serialize its `SelectParts` state, and cannot
    be fully deserialized without a custom implementation of `MappingReader`
    (which by default will deserialize a `SelectPartsLeaf` as a base `Leaf`
    instance, or raise if `extra` is not empty).
    """

    def __init__(self, *args: Any, select_parts: SelectParts[_T, _L], extra: Mapping[str, Any]):
        super().__init__(*args)
        self.select_parts = select_parts
        self.extra = extra

    def write_extra_to_mapping(self) -> Mapping[str, Any]:
        # Docstring inherited.
        return self.extra


@dataclasses.dataclass(eq=False, slots=True)
class ToSelectParts(RelationVisitor[_T, SelectParts[_T, _L]], Generic[_T, _L]):
    """A `.RelationVisitor` implemention that converts a `.Relation` tree into
    a `SelectParts` struct.

    This visitor directly handles `Leaf`, `operations.Join`,
    `operations.Projection`, and `operations.Selection` relations, and
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

    def visit_leaf(self, visited: Leaf[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        return cast(SelectPartsLeaf[_T, _L], visited).select_parts

    def visit_join(self, visited: operations.Join[_T]) -> SelectParts[_T, _L]:
        # Docstring inherited.
        if not visited.relations:
            return SelectParts(self.column_types.make_unit_subquery(), (), {})
        first_term, *other_terms = self._sorted_join_terms(visited.relations, visited.conditions)
        first_relation, first_condition = first_term
        assert not first_condition, "first relation should not have any join conditions"
        join_parts = first_relation.visit(self)
        if join_parts.columns_available is None:
            join_parts.columns_available = self.column_types.extract_mapping(
                first_relation.columns, join_parts.from_clause.columns
            )
        for term_relation, term_conditions in other_terms:
            join_parts = self._join_select_parts(join_parts, term_relation, term_conditions)
        return join_parts

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
        full_where = list(base_parts.where)
        for p in visited.predicates:
            full_where.extend(
                self.column_types.convert_predicate(visited.engine.tag, p, base_parts.columns_available)
            )
        return dataclasses.replace(base_parts, where=full_where)

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

    def _sorted_join_terms(
        self, relations: Sequence[Relation[_T]], conditions: Set[JoinCondition[_T]]
    ) -> Iterator[tuple[Relation[_T], set[JoinCondition[_T]]]]:
        """Sort the relations in a join operation to avoid Cartesian products
        (empty JOIN ON expressions) and associate join conditions with pairs
        of relations.

        Parameters
        ----------
        relations : `Sequence` [ `.Relation` ]
            Relations to sort.
        conditions : `~collections.abc.Set` [ `.JoinCondition` ]
            Special join conditions to associate with pairs of relations.

        Yields
        ------
        relation : `.Relation`
            A relation to join to all of those previously yielded.
        matching_conditions : `set` [ `.JoinCondition` ]
            Join conditions to apply when joining in this relation.
        """
        # We want to join terms into the SQL query in an order such that each
        # join's ON clause has something in common with the ones that preceded
        # it, and to find out if that's impossible and hence a Cartesian join
        # is needed.  Starting with the terms that have the most columns is a
        # good initial guess for such an ordering, so we begin by sort
        # relations by the number of columns they provide, in reverse, and put
        # them in a deque. Note that this does sort not take into account
        # special join conditions, but the rest of the logic will.
        relations_to_do = deque(sorted(relations, key=lambda r: len(r.columns)))
        # Make a mutable set of the special join conditions we need to use.
        conditions_to_do = set(conditions)
        assert len(relations_to_do) > 1, "No join needed for 0 or 1 clauses."
        # Start an outer loop over all relations.
        # We now refine the relation order, popping terms from the front of
        # `todo` and yielding them when we we have the kind of ON condition
        # we want.
        while relations_to_do:
            candidate = relations_to_do.popleft()
            yield candidate, set()
            columns_seen = set(candidate.columns)
            # A list of relations we haven't been able to join to columns_seen.
            # We'll move relations to `relations_rejected` from
            # `relations_to_do` and back in this inner loop, until we either
            # finish or everything gets rejected.
            relations_rejected: list[Relation[_T]] = []
            while relations_to_do:
                candidate = relations_to_do.popleft()
                # Find JoinConditions that match this candidate to
                # columns_seen.
                conditions_matched = JoinCondition.find_matching(
                    columns_seen, candidate.columns, conditions_to_do
                )
                if columns_seen.isdisjoint(candidate.columns) and not conditions_matched:
                    # We don't have any way to connect already seen columns to
                    # this relation.  We put this relation in the rejected list
                    # for now, and let the inner loop continue to try the next
                    # one.
                    relations_rejected.append(candidate)
                else:
                    # This candidate does have some column overlap.  In
                    # addition to yielding it with the matching conditions, we
                    # reset the rejected list by transferring its contents to
                    # the end of to_do, since this new term may have some
                    # column overlap with those we've previously rejected.
                    yield candidate, conditions_matched
                    columns_seen.update(candidate.columns)
                    relations_to_do.extend(relations_rejected)
                    relations_rejected.clear()
            if relations_rejected:
                # We've processed all relations that could be connected to the
                # starting one by at least one column or special JoinCondition.
                # In the future, we could guard against unintentional Cartesian
                # products here (see e.g. DM-33147), by checking for common
                # mistakes, emitting warnings, looking for some feature flag
                # that says to enable them, etc.  For now we just permit them.
                # But we still need to see if any of these "rejected" relations
                # can be connected to each other.  So we start the algorithm
                # again by returning to the outermost loop, with a fresh
                # to_do deque.
                relations_to_do.extend(relations_rejected)
                relations_rejected.clear()

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
            on_terms.extend(
                self.column_types.convert_join_condition(
                    next_relation.engine.tag,
                    condition,
                    (base_parts.columns_available, next_parts.columns_available),
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
