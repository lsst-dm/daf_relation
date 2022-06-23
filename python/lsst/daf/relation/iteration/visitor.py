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

__all__ = ("IterationVisitor",)

from typing import TYPE_CHECKING

from .._columns import _T
from .._exceptions import EngineError
from .._relation_visitor import RelationVisitor
from ._row_container import RowContainer
from ._row_iterable import RowIterable
from .chain import ChainRowIterable
from .joins import make_join_row_iterable
from .projection import ProjectionRowIterable
from .selection import SelectionRowIterable

if TYPE_CHECKING:
    from .. import operations
    from .._leaf import Leaf
    from ._typing import OrderByTermState


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
        base_relation, next_relation = visited.relations
        base_rows = base_relation.visit(self)
        next_rows = next_relation.visit(self)
        return make_join_row_iterable(base_rows, next_rows, base_relation, next_relation, visited.conditions)

    def visit_leaf(self, visited: Leaf[_T]) -> RowIterable[_T]:
        return visited.engine_state

    def visit_projection(self, visited: operations.Projection[_T]) -> RowIterable[_T]:
        return ProjectionRowIterable(visited.base.visit(self), tuple(visited.columns))

    def visit_selection(self, visited: operations.Selection[_T]) -> RowIterable[_T]:
        rows = visited.base.visit(self)
        rows, predicates_used = rows.try_selection(visited.engine.tag, visited.predicates)
        remaining_predicates = visited.predicates - predicates_used
        return SelectionRowIterable(
            rows, tuple(p.engine_state[visited.engine.tag] for p in remaining_predicates)
        )

    def visit_slice(self, visited: operations.Slice[_T]) -> RowIterable[_T]:
        base_rows = visited.base.visit(self)
        rows_list = list(base_rows)
        for order_by_term in visited.order_by[::-1]:
            sort_key: OrderByTermState[_T] = order_by_term.engine_state[visited.engine.tag]
            rows_list.sort(key=sort_key, reverse=not order_by_term.ascending)
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
