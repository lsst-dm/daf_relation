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
from ._row_iterable import RowCollection, RowIterable
from ._to_bool_callable import ToBoolCallable
from ._to_callable import ToCallable
from .chain import ChainRowIterable
from .joins import make_join_row_iterable
from .projection import ProjectionRowIterable
from .selection import SelectionRowIterable

if TYPE_CHECKING:
    from .. import operations
    from .._leaf import Leaf
    from .._relation import Identity, Zero
    from ._engine import Engine


class IterationVisitor(RelationVisitor[_T, RowIterable[_T]]):
    """The `.RelationVisitor` implementation for `Engine.execute`.

    This class should at most rarely need to be used directly, but it may be
    useful as a base class when specialized execution of native iteration is
    needed.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def visit_distinct(self, visited: operations.Distinct[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        base_rows = visited.visit(self)
        key_columns = next(iter(visited.unique_keys))  # don't care which unique key we use
        return base_rows.with_unique_index(key_columns)

    def visit_identity(self, visited: Identity[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        return RowCollection[_T]([{}])

    def visit_join(self, visited: operations.Join[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        lhs_rows = visited.lhs.visit(self)
        rhs_rows = visited.rhs.visit(self)
        return make_join_row_iterable(
            self.engine, lhs_rows, rhs_rows, visited.lhs, visited.rhs, visited.condition
        )

    def visit_materialization(self, visited: operations.Materialization[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        base_rows = visited.base.visit(self)
        if visited.unique_keys:
            return RowCollection.build_with_unique_index(base_rows, next(iter(visited.unique_keys)))
        else:
            return RowCollection(list(base_rows))

    def visit_leaf(self, visited: Leaf[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        return self.engine.evaluate_leaf(visited)

    def visit_projection(self, visited: operations.Projection[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        base_rows = visited.base.visit(self)
        return ProjectionRowIterable(base_rows, frozenset(visited.columns))

    def visit_selection(self, visited: operations.Selection[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        rows = visited.base.visit(self)
        return SelectionRowIterable(rows, visited.predicate.visit(ToBoolCallable(self.engine)))

    def visit_slice(self, visited: operations.Slice[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        base_rows = visited.base.visit(self)
        if (result := base_rows.try_slice(visited.order_by, visited.offset, visited.limit)) is not None:
            return result
        rows_list = list(base_rows)
        for order_by_term in visited.order_by[::-1]:
            rows_list.sort(
                key=order_by_term.expression.visit(ToCallable(self.engine)),
                reverse=not order_by_term.ascending,
            )
        if visited.limit is not None:
            stop = visited.offset + visited.limit
            rows_list = rows_list[visited.offset : stop]
        elif visited.offset:
            rows_list = rows_list[visited.offset :]
        return RowCollection(rows_list)

    def visit_transfer(self, visited: operations.Transfer) -> RowIterable[_T]:
        # Docstring inherited.
        raise EngineError("Native iteration only works on relation trees with no transfers.")

    def visit_union(self, visited: operations.Union[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        return ChainRowIterable([visited.first.visit(self), visited.second.visit(self)])

    def visit_zero(self, visited: Zero[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        return RowCollection[_T]([])
