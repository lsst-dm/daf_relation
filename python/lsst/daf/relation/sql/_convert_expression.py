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

__all__ = ("ConvertExpression",)

import dataclasses
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Generic, TypeVar, Union

import sqlalchemy

from .. import column_expressions
from .._columns import _T

if TYPE_CHECKING:
    from ._engine import Engine

_L = TypeVar("_L")


@dataclasses.dataclass(eq=False, slots=True)
class ConvertExpression(
    column_expressions.ExpressionVisitor[_T, _L],
    column_expressions.PredicateVisitor[_T, Sequence[sqlalchemy.sql.ColumnElement]],
    column_expressions.ContainerVisitor[_T, Union[Sequence[sqlalchemy.sql.ColumnElement], range]],
    Generic[_T, _L],
):

    engine: Engine[_T, _L]
    columns_available: Mapping[_T, _L]

    def visit_literal(self, visited: column_expressions.Literal[_T]) -> _L:
        return self.engine.convert_expression_literal(visited.value)

    def visit_reference(self, visited: column_expressions.Reference[_T]) -> _L:
        return self.columns_available[visited.tag]

    def visit_function(self, visited: column_expressions.Function[_T]) -> _L:
        if (function := self.engine.get_column_function(visited.name)) is not None:
            return function(*[arg.visit(self) for arg in visited.args])
        first, *rest = [arg.visit(self) for arg in visited.args]
        return getattr(first, visited.name)(*rest)

    def visit_in_container(
        self, visited: column_expressions.InContainer[_T]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        lhs = self.engine.expect_expression_scalar(visited.lhs.visit(self))
        rhs = visited.rhs.visit(self)
        match rhs:
            case range(start=start, stop=stop_exclusive, step=step):
                # The convert_expression_literal calls here should just call
                # sqlalchemy.sql.literal(int), which would also happen
                # automatically internal to any of the other sqlalchemy
                # function calls, but they get the typing right, reflecting the
                # fact that the engine is supposed to have final say over how
                # we convert literals.
                stop_inclusive = stop_exclusive - 1
                if start == stop_inclusive:
                    return [lhs == self.engine.convert_expression_literal(start)]
                else:
                    base = sqlalchemy.sql.between(
                        lhs,
                        self.engine.convert_expression_literal(start),
                        self.engine.convert_expression_literal(stop_inclusive),
                    )
                    if step != 1:
                        return [
                            base,
                            lhs % self.engine.convert_expression_literal(step)
                            == self.engine.convert_expression_literal(start % step),
                        ]
                    else:
                        return [base]
        return [lhs.in_(rhs)]

    def visit_predicate_literal(
        self, visited: column_expressions.PredicateLiteral[_T]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        return [sqlalchemy.sql.literal(visited.value)]

    def visit_predicate_reference(
        self, visited: column_expressions.PredicateReference[_T]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        return [self.columns_available[visited.tag]]

    def visit_predicate_function(
        self, visited: column_expressions.PredicateFunction[_T]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        if (function := self.engine.get_column_function(visited.name)) is not None:
            return (function(*[arg.visit(self) for arg in visited.args]),)
        first, *rest = [arg.visit(self) for arg in visited.args]
        return [getattr(first, visited.name)(*rest)]

    def visit_logical_not(
        self, visited: column_expressions.LogicalNot[_T]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        return [sqlalchemy.sql.not_(self._and_if_needed(visited.base.visit(self)))]

    def visit_logical_and(
        self, visited: column_expressions.LogicalAnd[_T]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        result: list[sqlalchemy.sql.ColumnElement] = []
        for operand in visited.operands:
            nested = operand.visit(self)
            result.extend(nested)
        return result

    def visit_logical_or(
        self, visited: column_expressions.LogicalOr[_T]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        terms = [self._and_if_needed(operand.visit(self)) for operand in visited.operands]
        if not terms:
            return [sqlalchemy.sql.literal(False)]
        if len(terms) == 1:
            return [terms[0]]
        else:
            return [sqlalchemy.sql.or_(*terms)]

    def visit_range_literal(
        self, visited: column_expressions.RangeLiteral[_T]
    ) -> Sequence[sqlalchemy.sql.ColumnElement] | range:
        return visited.value

    def visit_expression_sequence(
        self, visited: column_expressions.ExpressionSequence[_T]
    ) -> Sequence[sqlalchemy.sql.ColumnElement] | range:
        return [self.engine.expect_expression_scalar(item.visit(self)) for item in visited.items]

    def _and_if_needed(self, items: Sequence[sqlalchemy.sql.ColumnElement]) -> sqlalchemy.sql.ColumnElement:
        if not items:
            return sqlalchemy.sql.literal(True)
        if len(items) == 1:
            return items[0]
        else:
            return sqlalchemy.sql.and_(*items)
