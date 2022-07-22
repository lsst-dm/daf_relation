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

__all__ = ("ToCallable",)

import dataclasses
import operator
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .. import column_expressions
from .._columns import _T
from .._exceptions import EngineError
from .typing import Row

if TYPE_CHECKING:
    from ._engine import Engine


@dataclasses.dataclass(eq=False, slots=True)
class ToCallable(column_expressions.ExpressionVisitor[_T, Callable[[Row[_T]], Any]]):

    engine: Engine[_T]

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
        raise EngineError(f"Expression function {visited.name!r} is not supported by engine {self.engine}.")
