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

__all__ = ("ToBoolCallable",)

import dataclasses
import operator
from typing import TYPE_CHECKING, Callable

from .. import column_expressions
from .._columns import _T
from ._to_callable import ToCallable
from .typing import Row

if TYPE_CHECKING:
    from ._engine import Engine


@dataclasses.dataclass(eq=False, slots=True)
class ToBoolCallable(column_expressions.PredicateVisitor[_T, Callable[[Row[_T]], bool]]):
    def __init__(self, engine: Engine[_T]):
        self.to_callable = ToCallable(engine)

    to_callable: ToCallable[_T]

    @property
    def engine(self) -> Engine:
        return self.to_callable.engine

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
            arg_callables = [arg.visit(self.to_callable) for arg in visited.args]
            # MyPy doesn't see 'function' as not-None in the capture for some
            # reason.
            return lambda row: function(*[c(row) for c in arg_callables])  # type: ignore
        first, *rest = [arg.visit(self.to_callable) for arg in visited.args]
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
