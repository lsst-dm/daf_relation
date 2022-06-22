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

__all__ = ()

from typing import TYPE_CHECKING, AbstractSet

from .._column_tag import _T
from .._exceptions import EngineError
from .._join_condition import JoinCondition
from .._relation_visitor import RelationVisitor
from ._row_iterable import (
    ChainRowIterable,
    GeneralJoinRowIterable,
    JoinConditionSelectionRowIterable,
    ProjectionRowIterable,
    RowContainer,
    RowIterable,
    SelectionRowIterable,
    UniqueIndexJoinRowIterable,
)

if TYPE_CHECKING:
    from .. import operations
    from .._engines import EngineTag
    from .._leaf import Leaf
    from .._relation import Relation


class IterationVisitor(RelationVisitor[_T, RowIterable[_T]]):
    def visit_distinct(self, visited: operations.Distinct[_T]) -> RowIterable[_T]:
        base_rows = visited.visit(self)
        key_columns = next(iter(visited.unique_keys))  # don't care which unique key we use
        return base_rows.with_unique_index(key_columns)

    def visit_join(self, visited: operations.Join[_T]) -> RowIterable[_T]:
        if len(visited.relations) == 0:
            return RowContainer([])
        if len(visited.relations) == 1:
            return visited.relations[0].visit(self)
        if len(visited.relations) > 2:
            raise EngineError("Native iteration requires a tree with only pairwise joins.")
        rows0 = visited.relations[0].visit(self)
        rows1 = visited.relations[1].visit(self)
        if rows0.materialization <= rows1.materialization:
            rel0 = visited.relations[0]
            rel1 = visited.relations[1]
        else:
            (rows0, rows1) = (rows1, rows0)
            rel0 = visited.relations[1]
            rel1 = visited.relations[0]
        all_columns = rel0.columns | rel1.columns
        join_rows, matched_conditions = rows1.try_join(rel1, rows0, rel0, visited.conditions)
        if join_rows is not None:
            return self._finish_join(
                visited.engine.tag, join_rows, all_columns, visited.conditions - matched_conditions
            )
        if rows0.materialization == rows1.materialization:
            join_rows, matched_conditions = rows0.try_join(rel0, rows1, rel1, visited.conditions)
            if join_rows is not None:
                return self._finish_join(
                    visited.engine.tag, join_rows, all_columns, visited.conditions - matched_conditions
                )
        common_columns = frozenset(rel0.columns & rel1.columns)
        if rel1.is_unique_key_covered(common_columns):
            return self._finish_join(
                visited.engine.tag,
                UniqueIndexJoinRowIterable(rows0, rows1, on_key=common_columns),
                all_columns,
                visited.conditions,
            )
        if rows0.materialization == rows1.materialization and rel0.is_unique_key_covered(common_columns):
            return self._finish_join(
                visited.engine.tag,
                UniqueIndexJoinRowIterable(rows1, rows0, on_key=common_columns),
                all_columns,
                visited.conditions,
            )
        return self._finish_join(
            visited.engine.tag,
            GeneralJoinRowIterable(rows0, rows1, on_key=common_columns),
            all_columns,
            visited.conditions,
        )

    def _finish_join(
        self,
        engine: EngineTag,
        base: RowIterable[_T],
        all_columns: AbstractSet[_T],
        missing_conditions: AbstractSet[JoinCondition[_T]],
    ) -> RowIterable[_T]:
        if not missing_conditions:
            return base
        return JoinConditionSelectionRowIterable(base, tuple(c.state[engine] for c in missing_conditions))

    @staticmethod
    def _make_join_rows(
        base_relation: Relation[_T],
        base_rows: RowIterable[_T],
        next_relation: Relation[_T],
        next_rows: RowIterable[_T],
        matching_conditions: AbstractSet[JoinCondition[_T]],
    ) -> tuple[RowIterable[_T], AbstractSet[JoinCondition[_T]]]:
        join_rows, matched_conditions = next_rows.try_join(
            next_relation, base_rows, base_relation, matching_conditions
        )
        if join_rows is not None:
            return join_rows, matched_conditions
        common_columns = frozenset(base_relation.columns & next_relation.columns)
        if next_relation.is_unique_key_covered(common_columns):
            return UniqueIndexJoinRowIterable(base_rows, next_rows, on_key=common_columns), frozenset()
        else:
            return GeneralJoinRowIterable(base_rows, next_rows, on_key=common_columns), frozenset()

    def visit_leaf(self, visited: Leaf[_T]) -> RowIterable[_T]:
        return visited.state

    def visit_projection(self, visited: operations.Projection[_T]) -> RowIterable[_T]:
        return ProjectionRowIterable(visited.base.visit(self), tuple(visited.columns))

    def visit_selection(self, visited: operations.Selection[_T]) -> RowIterable[_T]:
        rows = visited.base.visit(self)
        rows, predicates_used = rows.try_selection(visited.engine.tag, visited.predicates)
        remaining_predicates = visited.predicates - predicates_used
        return SelectionRowIterable(rows, tuple(p.state[visited.engine.tag] for p in remaining_predicates))

    def visit_slice(self, visited: operations.Slice[_T]) -> RowIterable[_T]:
        base_rows = visited.base.visit(self)
        rows_list = list(base_rows)
        for order_by_term in visited.order_by[::-1]:
            rows_list.sort(key=order_by_term.state[visited.engine.tag], reverse=not order_by_term.ascending)
        if visited.limit is not None:
            stop = visited.offset + visited.limit
            rows_list = rows_list[visited.offset : stop]
        elif visited.offset:
            rows_list = rows_list[visited.offset :]
        return RowContainer(rows_list)

    def visit_transfer(self, visited: operations.Transfer) -> RowIterable[_T]:
        raise EngineError("Native iteration only works on relation trees with no transfers.")

    def visit_union(self, visited: operations.Union[_T]) -> RowIterable[_T]:
        if len(visited.relations) == 0:
            return RowContainer([])
        if len(visited.relations) == 1:
            return visited.relations[0].visit(self)
        return ChainRowIterable([r.visit(self) for r in visited.relations])
