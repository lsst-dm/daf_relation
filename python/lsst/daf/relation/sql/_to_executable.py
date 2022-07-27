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

__all__ = ("ToExecutable",)

import dataclasses
from collections.abc import Sequence
from typing import TYPE_CHECKING, Generic, TypeVar

import sqlalchemy

from .. import operations
from .._columns import _T
from .._exceptions import EngineError
from .._relation_visitor import RelationVisitor
from ._select_parts import ToSelectParts

if TYPE_CHECKING:
    from .. import column_expressions
    from .._leaf import Leaf
    from .._relation import Doomed, Identity, Relation
    from ._engine import Engine


_L = TypeVar("_L")


@dataclasses.dataclass(eq=False)
class ToExecutable(RelationVisitor[_T, sqlalchemy.sql.expression.SelectBase], Generic[_T, _L]):
    """A `.RelationVisitor` implemention that converts a `.Relation` tree into
    a SQLAlchemy (possibly-compound) SELECT query that can be directly
    executed.

    This visitor directly handles `.Extension`, `.operations.Slice`,
    `.operations.Union`, and `.operations.Distinct` relations, and delegates
    the others to `ToSelectParts`.  It does not handle transfers at all.
    """

    engine: Engine[_T, _L]
    # TODO docs.

    distinct: bool = False
    """Whether to force the rows of the final SQL executable returned to be
    unique, via SELECT DISTINCT or UNION (`bool`).
    """

    order_by: Sequence[column_expressions.OrderByTerm[_T]] = ()
    """Terms to sort the rows of the final SQL executable
    (`Sequence` [ `.column_expressions.OrderByTerm` )."""

    offset: int = 0
    """Offset of the first row returned from the query, starting from zero
    (`int`).
    """

    limit: int | None = None
    """Maximum number of rows returned by the query (`int` or `None`).
    """

    def visit_calculation(self, visited: operations.Calculation[_T]) -> sqlalchemy.sql.expression.SelectBase:
        return self._use_select_parts(visited)

    def visit_distinct(self, visited: operations.Distinct[_T]) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        return visited.base.visit(dataclasses.replace(self, distinct=True))

    def visit_doomed(self, visited: Doomed[_T]) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        return self.engine.make_doomed_select(visited.columns)

    def visit_identity(self, visited: Identity[_T]) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        return self._use_select_parts(visited)

    def visit_leaf(self, visited: Leaf[_T]) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        return self._use_select_parts(visited)

    def visit_materialization(
        self, visited: operations.Materialization[_T]
    ) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        return self._use_select_parts(visited)

    def visit_join(self, visited: operations.Join[_T]) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        return self._use_select_parts(visited)

    def visit_projection(self, visited: operations.Projection[_T]) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        return self._use_select_parts(visited)

    def visit_selection(self, visited: operations.Selection[_T]) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        return self._use_select_parts(visited)

    def visit_slice(self, visited: operations.Slice[_T]) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        if self.order_by or self.offset or self.limit is not None:
            # This visitor wants to impose its own slice operations on the
            # final result.  Delegate to _use_select_parts, which will delegate
            # back here using a visitor without any slice operations, and then
            # wrap that in a subquery.
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
        # Docstring inherited.
        raise EngineError("SQL conversion only works on relation trees with no transfers.")

    def visit_union(self, visited: operations.Union[_T]) -> sqlalchemy.sql.expression.SelectBase:
        # Docstring inherited.
        nested_visitor = dataclasses.replace(self, distinct=False, order_by=False, offset=0, limit=None)
        new_first = visited.lhs.visit(nested_visitor)
        new_second = visited.rhs.visit(nested_visitor)
        executable: sqlalchemy.sql.CompoundSelect = (
            sqlalchemy.sql.union(new_first, new_second)
            if self.distinct
            else sqlalchemy.sql.union_all(new_first, new_second)
        )
        if self.order_by:
            columns_available = self.engine.extract_mapping(
                visited.columns,
                executable.selected_columns,
            )
            executable = executable.order_by(
                *[self.engine.convert_order_by(term, columns_available) for term in self.order_by]
            )
        if self.offset:
            executable = executable.offset(self.offset)
        if self.limit is not None:
            executable = executable.limit(self.limit)
        return executable

    def _use_select_parts(self, relation: Relation[_T]) -> sqlalchemy.sql.Select:
        """Delegate to `ToSelectParts` to implement visitation for a relation.

        Parameters
        ----------
        relation : `.Relation`
            Relation to process.

        Returns
        -------
        select : `sqlalchemy.sql.Select`
            SQL SELECT statement.
        """
        select_parts = relation.visit(ToSelectParts(self.engine))
        return select_parts.to_executable(
            relation,
            self.engine,
            distinct=self.distinct,
            order_by=self.order_by,
            offset=self.offset,
            limit=self.limit,
        )
