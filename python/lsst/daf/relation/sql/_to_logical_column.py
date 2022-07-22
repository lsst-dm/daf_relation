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

__all__ = ("ToLogicalColumn",)

import dataclasses
from collections.abc import Mapping
from typing import TYPE_CHECKING, TypeVar

from .._columns import _T
from .._exceptions import EngineError

if TYPE_CHECKING:
    from .. import column_expressions
    from ._engine import Engine


_L = TypeVar("_L")


@dataclasses.dataclass(eq=False, slots=True)
class ToLogicalColumn(column_expressions.ExpressionVisitor[_T, _L]):

    engine: Engine[_L]
    columns_available: Mapping[_T, _L]

    def visit_literal(self, visited: column_expressions.Literal[_T]) -> _L:
        return self.engine.convert_expression_literal(visited.value)

    def visit_reference(self, visited: column_expressions.Reference[_T]) -> _L:
        return self.columns_available[visited.tag]

    def visit_function(self, visited: column_expressions.Function[_T]) -> _L:
        if (function := self.engine.get_column_function(visited.name)) is not None:
            return function(*[arg.visit(self) for arg in visited.args])
        raise EngineError(f"Expression function {visited.name!r} is not supported by engine {self.engine}.")
