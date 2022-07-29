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

import operator
from collections.abc import Container
from typing import TYPE_CHECKING, Any, Callable

from .. import column_expressions
from .._columns import _T
from .._exceptions import EngineError
from .._relation_visitor import RelationVisitor
from ._row_iterable import RowCollection, RowIterable
from .calculation import CalculationRowIterable
from .chain import ChainRowIterable
from .joins import make_join_row_iterable
from .projection import ProjectionRowIterable
from .selection import SelectionRowIterable
from .typing import Row

if TYPE_CHECKING:
    from .. import operations
    from .._leaf import Leaf
    from .._relation import Doomed, Identity
    from ._engine import Engine


class IterationVisitor(
    RelationVisitor[_T, RowIterable[_T]],
    column_expressions.ExpressionVisitor[_T, Callable[[Row[_T]], Any]],
    column_expressions.PredicateVisitor[_T, Callable[[Row[_T]], bool]],
    column_expressions.ContainerVisitor[_T, Callable[[Row[_T]], Container]],
):
    """The `.RelationVisitor` implementation for `Engine.execute`.

    This class should at most rarely need to be used directly, but it may be
    useful as a base class when specialized execution of native iteration is
    needed.
    """

    def __init__(self, engine: Engine[_T]):
        self.engine = engine

    def visit_calculation(self, visited: operations.Calculation[_T]) -> RowIterable[_T]:
        callable = visited.expression.visit(self)
        return CalculationRowIterable(visited.base.visit(self), visited.tag, callable)

    def visit_distinct(self, visited: operations.Distinct[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        base_rows = visited.visit(self)
        key_columns = next(iter(visited.unique_keys))  # don't care which unique key we use
        return base_rows.with_unique_index(key_columns)

    def visit_doomed(self, visited: Doomed[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        return RowCollection[_T]([])

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
        return SelectionRowIterable(rows, visited.predicate.visit(self))

    def visit_slice(self, visited: operations.Slice[_T]) -> RowIterable[_T]:
        # Docstring inherited.
        base_rows = visited.base.visit(self)
        if (result := base_rows.try_slice(visited.order_by, visited.offset, visited.limit)) is not None:
            return result
        rows_list = list(base_rows)
        for order_by_term in visited.order_by[::-1]:
            rows_list.sort(
                key=order_by_term.expression.visit(self),
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
        return ChainRowIterable([visited.lhs.visit(self), visited.rhs.visit(self)])

    def visit_literal(self, visited: column_expressions.Literal[_T]) -> Callable[[Row[_T]], Any]:
        return lambda row: visited.value

    def visit_reference(self, visited: column_expressions.Reference[_T]) -> Callable[[Row[_T]], Any]:
        return operator.itemgetter(visited.tag)

    def visit_function(self, visited: column_expressions.Function[_T]) -> Callable[[Row[_T]], Any]:
        function = self.engine.get_column_function(visited.name)
        if function is not None:
            arg_callables = [arg.visit(self) for arg in visited.args]
            # MyPy doesn't see 'function' as not-None for some reason.
            return lambda row: function(*[c(row) for c in arg_callables])  # type: ignore
        first, *rest = [arg.visit(self) for arg in visited.args]
        return lambda row: getattr(first(row), visited.name)(*[r(row) for r in rest])

    def visit_in_container(self, visited: column_expressions.InContainer[_T]) -> Callable[[Row[_T]], bool]:
        lhs_callable = visited.lhs.visit(self)
        rhs_callable: Callable[[Row], Container] = visited.rhs.visit(self)
        return lambda row: lhs_callable(row) in rhs_callable(row)

    def visit_predicate_literal(
        self, visited: column_expressions.PredicateLiteral[_T]
    ) -> Callable[[Row[_T]], bool]:
        return lambda row: visited.value

    def visit_predicate_reference(
        self, visited: column_expressions.PredicateReference[_T]
    ) -> Callable[[Row[_T]], bool]:
        return operator.itemgetter(visited.tag)

    def visit_predicate_function(
        self, visited: column_expressions.PredicateFunction[_T]
    ) -> Callable[[Row[_T]], bool]:
        function = self.engine.get_column_function(visited.name)
        if function is not None:
            arg_callables = [arg.visit(self) for arg in visited.args]
            # MyPy doesn't see 'function' as not-None in the capture for some
            # reason.
            return lambda row: function(*[c(row) for c in arg_callables])  # type: ignore
        first, *rest = [arg.visit(self) for arg in visited.args]
        return lambda row: getattr(first(row), visited.name)(*[r(row) for r in rest])

    def visit_logical_not(self, visited: column_expressions.LogicalNot[_T]) -> Callable[[Row[_T]], bool]:
        base_callable = visited.base.visit(self)
        return lambda row: not base_callable(row)

    def visit_logical_and(self, visited: column_expressions.LogicalAnd[_T]) -> Callable[[Row[_T]], bool]:
        operand_callables = [arg.visit(self) for arg in visited.operands]
        return lambda row: all(c(row) for c in operand_callables)

    def visit_logical_or(self, visited: column_expressions.LogicalOr[_T]) -> Callable[[Row[_T]], bool]:
        operand_callables = [arg.visit(self) for arg in visited.operands]
        return lambda row: any(c(row) for c in operand_callables)

    def visit_range_literal(
        self, visited: column_expressions.RangeLiteral[_T]
    ) -> Callable[[Row[_T]], Container]:
        return lambda row: visited.value

    def visit_expression_sequence(
        self, visited: column_expressions.ExpressionSequence[_T]
    ) -> Callable[[Row[_T]], Container]:
        item_callables = [item.visit(self) for item in visited.items]
        return lambda row: {c(row) for c in item_callables}
