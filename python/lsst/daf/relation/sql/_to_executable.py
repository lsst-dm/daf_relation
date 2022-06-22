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

__all__ = ("to_executable", "to_from_clause")

import dataclasses
from typing import TYPE_CHECKING, Generic, Sequence

import sqlalchemy

from .. import operations
from .._columns import _T
from .._exceptions import EngineError
from .._relation_visitor import RelationVisitor
from ._column_type_info import _L, ColumnTypeInfo
from ._select_parts import SelectParts

if TYPE_CHECKING:
    from .._leaf import Leaf
    from .._order_by_term import OrderByTerm
    from .._relation import Relation


def to_executable(
    relation: Relation[_T],
    column_types: ColumnTypeInfo[_T, _L],
    *,
    distinct: bool = False,
    order_by: Sequence[OrderByTerm[_T]] = (),
    offset: int = 0,
    limit: int | None = None,
) -> sqlalchemy.sql.expression.SelectBase:
    return relation.visit(_ToExecutable(column_types, distinct, order_by, offset, limit))


def to_from_clause(relation: Relation[_T], column_types: ColumnTypeInfo[_T, _L]) -> sqlalchemy.sql.FromClause:
    select_parts = SelectParts.from_relation(relation, column_types)
    if not select_parts.where and select_parts.columns_available is None:
        return select_parts.from_clause
    return select_parts.to_executable(relation, column_types).subquery()


@dataclasses.dataclass(eq=False, slots=True)
class _ToExecutable(RelationVisitor[_T, sqlalchemy.sql.expression.SelectBase], Generic[_T, _L]):

    column_types: ColumnTypeInfo[_T, _L]
    distinct: bool = False
    order_by: Sequence[OrderByTerm[_T]] = ()
    offset: int = 0
    limit: int | None = None

    def visit_distinct(self, visited: operations.Distinct[_T]) -> sqlalchemy.sql.expression.SelectBase:
        return visited.base.visit(dataclasses.replace(self, distinct=True))

    def visit_leaf(self, visited: Leaf[_T]) -> sqlalchemy.sql.expression.SelectBase:
        return self._use_select_parts(visited)

    def visit_join(self, visited: operations.Join[_T]) -> sqlalchemy.sql.expression.SelectBase:
        return self._use_select_parts(visited)

    def visit_projection(self, visited: operations.Projection[_T]) -> sqlalchemy.sql.expression.SelectBase:
        return self._use_select_parts(visited)

    def visit_selection(self, visited: operations.Selection[_T]) -> sqlalchemy.sql.expression.SelectBase:
        return self._use_select_parts(visited)

    def visit_slice(self, visited: operations.Slice[_T]) -> sqlalchemy.sql.expression.SelectBase:
        if self.order_by or self.offset or self.limit is not None:
            # This visitor wants to impose its own slice operations on the
            # final result.  Delegate to SelectParts, which will delegate back
            # here using a visitor without any slice operations, and then wrap
            # that in a subquery.
            return self._use_select_parts(visited)
        else:
            # This visitor doesn't apply any slice operations, so we can use a
            # visitor that uses the slice's operations on its base.  If that
            # base is itself a Slice, then we'll end up back in the `if` block
            # above, which will recurse back to this `else` block - but each
            # time we recurse, we pop one `Slice` off, and eventually we'll
            # hit some other kind of relation.
            return visited.base.visit(
                dataclasses.replace(
                    self, order_by=visited.order_by, offset=visited.offset, limit=visited.limit
                )
            )

    def visit_transfer(self, visited: operations.Transfer) -> sqlalchemy.sql.expression.SelectBase:
        raise EngineError("SQL conversion only works on relation trees with no transfers.")

    def visit_union(self, visited: operations.Union[_T]) -> sqlalchemy.sql.expression.SelectBase:
        if not visited.relations:
            return self.column_types.make_zero_select(visited.columns)
        nested_visitor = dataclasses.replace(self, distinct=False, order_by=False, offset=0, limit=None)
        nested_executables = [r.visit(nested_visitor) for r in visited.relations]
        executable: sqlalchemy.sql.CompoundSelect = (
            sqlalchemy.sql.union(*nested_executables)
            if self.distinct
            else sqlalchemy.sql.union_all(*nested_executables)
        )
        if self.order_by:
            columns_available = self.column_types.extract_mapping(
                visited.columns,
                executable.selected_columns,
            )
            executable = executable.order_by(
                *[
                    self.column_types.convert_order_by(visited.engine.tag, t, columns_available)
                    for t in self.order_by
                ]
            )
        if self.offset:
            executable = executable.offset(self.offset)
        if self.limit is not None:
            executable = executable.limit(self.limit)
        return executable

    def _use_select_parts(self, relation: Relation[_T]) -> sqlalchemy.sql.Select:
        select_parts = SelectParts.from_relation(relation, self.column_types)
        return select_parts.to_executable(
            relation,
            self.column_types,
            distinct=self.distinct,
            order_by=self.order_by,
            offset=self.offset,
            limit=self.limit,
        )
