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

__all__ = ("SelectParts", "SelectPartsLeaf")

import dataclasses
from collections import deque
from typing import TYPE_CHECKING, AbstractSet, Any, Generic, Iterable, Iterator, Mapping, Sequence, cast

import sqlalchemy

from .. import operations
from .._columns import _T
from .._exceptions import EngineError
from .._leaf import Leaf
from .._relation_visitor import RelationVisitor
from ._column_type_info import _L, ColumnTypeInfo

if TYPE_CHECKING:
    from .._join_condition import JoinCondition
    from .._order_by_term import OrderByTerm
    from .._relation import Relation


@dataclasses.dataclass(slots=True, eq=False)
class SelectParts(Generic[_T, _L]):
    from_clause: sqlalchemy.sql.FromClause
    where: Sequence[sqlalchemy.sql.ColumnElement]
    columns_available: Mapping[_T, _L] | None

    @classmethod
    def from_relation(
        cls, relation: Relation[_T], column_types: ColumnTypeInfo[_T, _L]
    ) -> SelectParts[_T, _L]:
        return relation.visit(_ToSelectParts(column_types))

    def to_executable(
        self,
        relation: Relation[_T],
        column_types: ColumnTypeInfo[_T, _L],
        *,
        distinct: bool = False,
        order_by: Iterable[OrderByTerm[_T]] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> sqlalchemy.sql.Select:
        if self.columns_available is None:
            columns_available = column_types.extract_mapping(relation.columns, self.from_clause.columns)
            columns_projected = columns_available
        else:
            columns_available = self.columns_available
            columns_projected = {tag: columns_available[tag] for tag in relation.columns}
        select = column_types.select_items(columns_projected.items(), self.from_clause)
        if len(self.where) == 1:
            select = select.where(self.where[0])
        elif self.where:
            select = select.where(sqlalchemy.sql.and_(*self.where))
        if distinct:
            select = select.distinct()
        if order_by:
            select = select.order_by(
                *[
                    column_types.convert_order_by(
                        relation.engine.tag, t, cast(Mapping[_T, _L], self.columns_available)
                    )
                    for t in order_by
                ]
            )
        if offset:
            select = select.offset(offset)
        if limit is not None:
            select = select.limit(limit)
        return select

    def join(
        self,
        relation: Relation[_T],
        column_types: ColumnTypeInfo[_T, _L],
        conditions: Iterable[JoinCondition[_T]],
    ) -> SelectParts[_T, _L]:
        assert self.columns_available is not None
        relation_parts = relation.visit(_ToSelectParts(column_types))
        if relation_parts.columns_available is None:
            relation_parts.columns_available = column_types.extract_mapping(
                relation.columns, relation_parts.from_clause.columns
            )
        on_terms: list[sqlalchemy.sql.ColumnElement] = []
        for tag in self.columns_available.keys() & relation_parts.columns_available.keys():
            on_terms.append(self.columns_available[tag] == relation_parts.columns_available[tag])
        for condition in conditions:
            on_terms.extend(
                column_types.convert_join_condition(
                    relation.engine.tag, condition, (self.columns_available, relation_parts.columns_available)
                )
            )
        on_clause: sqlalchemy.sql.ColumnElement
        if not on_terms:
            on_clause = sqlalchemy.sql.literal(True)
        elif len(on_terms) == 1:
            on_clause = on_terms[0]
        else:
            on_clause = sqlalchemy.sql.and_(*on_terms)
        from_clause = self.from_clause.join(relation_parts.from_clause, onclause=on_clause)
        where = tuple(self.where) + tuple(relation_parts.where)
        columns_available = {**self.columns_available, **relation_parts.columns_available}
        return SelectParts(from_clause, where, columns_available)


class SelectPartsLeaf(Leaf[_T], Generic[_T, _L]):
    def __init__(self, *args: Any, select_parts: SelectParts[_T, _L]):
        super().__init__(*args)
        self.select_parts = select_parts


@dataclasses.dataclass(eq=False, slots=True)
class _ToSelectParts(RelationVisitor[_T, SelectParts[_T, _L]], Generic[_T, _L]):
    column_types: ColumnTypeInfo[_T, _L]

    def visit_distinct(self, visited: operations.Distinct[_T]) -> SelectParts[_T, _L]:
        return SelectParts(
            self._to_executable(visited).subquery(),
            [],
            None,
        )

    def visit_leaf(self, visited: Leaf[_T]) -> SelectParts[_T, _L]:
        return cast(SelectPartsLeaf[_T, _L], visited).select_parts

    def visit_join(self, visited: operations.Join[_T]) -> SelectParts[_T, _L]:
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
            join_parts = join_parts.join(term_relation, self.column_types, term_conditions)
        return join_parts

    def visit_projection(self, visited: operations.Projection[_T]) -> SelectParts[_T, _L]:
        # We can just delegate to base because projection only affects
        # to_executable, and the default implementation for that already only
        # selects the relation's own columns.
        return visited.base.visit(self)

    def visit_selection(self, visited: operations.Selection[_T]) -> SelectParts[_T, _L]:
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
        return SelectParts(
            self._to_executable(visited).subquery(),
            [],
            None,
        )

    def visit_transfer(self, visited: operations.Transfer) -> SelectParts[_T, _L]:
        raise EngineError("SQL conversion only works on relation trees with no transfers.")

    def visit_union(self, visited: operations.Union[_T]) -> SelectParts[_T, _L]:
        return SelectParts(
            self._to_executable(visited).subquery(),
            [],
            None,
        )

    def _to_executable(self, relation: Relation[_T]) -> sqlalchemy.sql.expression.SelectBase:
        from .to_executable import ToExecutable

        return relation.visit(ToExecutable(self.column_types))

    def _sorted_join_terms(
        self, relations: Sequence[Relation[_T]], conditions: AbstractSet[JoinCondition[_T]]
    ) -> Iterator[tuple[Relation[_T], set[JoinCondition[_T]]]]:
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
