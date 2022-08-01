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

__all__ = ("Diagnostics", "DiagnosticsVisitor")

import dataclasses
from typing import TYPE_CHECKING, Generic

from . import operations
from ._columns import _T
from ._relation import Doomed, Identity, Relation, UnaryOperation
from ._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from ._leaf import Leaf


@dataclasses.dataclass
class Diagnostics(Generic[_T]):
    is_doomed: bool
    messages: list[str]
    relation: Relation[_T]


class DiagnosticsVisitor(RelationVisitor[_T, Diagnostics[_T]]):
    def visit_calculation(self, visited: operations.Calculation[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        return self._visit_unary(f"Calculation operation (column {visited.tag!r})", visited)

    def visit_distinct(self, visited: operations.Distinct[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        return self._visit_unary(f"Distinct operation (with unique keys {visited.unique_keys})", visited)

    def visit_doomed(self, visited: Doomed[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        return Diagnostics(True, list(visited.messages), visited)

    def visit_identity(self, visited: Identity[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        return Diagnostics(False, [], visited)

    def visit_leaf(self, visited: Leaf[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        return Diagnostics(False, [], visited)

    def visit_materialization(self, visited: operations.Materialization[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        return self._visit_unary(f"Materialization (with name {visited.name!r})", visited)

    def visit_join(self, visited: operations.Join[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        lhs_result = visited.lhs.visit(self)
        rhs_result = visited.rhs.visit(self)
        new_messages = []
        base_message = f"Join (on condition {visited.condition}"
        if lhs_result.is_doomed:
            if rhs_result.is_doomed:
                new_messages.append(base_message + " is doomed because both operands are doomed:")
            else:
                new_messages.append(base_message + " is doomed because the LHS operand is doomed:")
        else:
            if rhs_result.is_doomed:
                new_messages.append(base_message + " is doomed because the RHS operand is doomed:")
            else:
                if lhs_result.messages or rhs_result.messages:
                    new_messages.append(base_message + " is not doomed, but some contributions were:")
        new_messages.extend("  " + message for message in lhs_result.messages)
        new_messages.extend("  " + message for message in rhs_result.messages)
        if lhs_result.is_doomed or rhs_result.is_doomed:
            return Diagnostics(
                True,
                new_messages,
                Doomed(visited.engine, visited.columns, new_messages),
            )
        else:
            return Diagnostics(False, new_messages, visited.rebase(lhs_result.relation, rhs_result.relation))

    def visit_projection(self, visited: operations.Projection[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        return self._visit_unary(f"Projection (to columns {set(visited.columns)})", visited)

    def visit_selection(self, visited: operations.Selection[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        return self._visit_unary(f"Selection (with predicate {visited.predicate})", visited)

    def visit_slice(self, visited: operations.Slice[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        return self._visit_unary(
            f"Slice (ordered by {visited.order_by}, offset={visited.offset}, limit={visited.limit})", visited
        )

    def visit_transfer(self, visited: operations.Transfer) -> Diagnostics[_T]:
        # Docstring inherited.
        return self._visit_unary(f"Transfer (to engine {visited.engine})", visited)

    def visit_union(self, visited: operations.Union[_T]) -> Diagnostics[_T]:
        # Docstring inherited.
        lhs_result = visited.lhs.visit(self)
        rhs_result = visited.rhs.visit(self)
        new_messages = []
        base_message = f"Union (with unique_keys {visited.unique_keys}"
        new_relation: Relation[_T] | None = None
        if lhs_result.is_doomed:
            if rhs_result.is_doomed:
                new_messages.append(base_message + " is doomed because both operands are doomed:")
            else:
                new_messages.append(base_message + " collapses to the RHS because the LHS is doomed:")
                new_relation = rhs_result.relation
        else:
            if rhs_result.is_doomed:
                new_messages.append(base_message + " collapses to the LHS because the RHS is doomed:")
                new_relation = lhs_result.relation
            else:
                if lhs_result.messages or rhs_result.messages:
                    new_messages.append(base_message + " is not doomed, but some contributions were:")
                new_relation = visited.rebase(lhs_result.relation, rhs_result.relation)
        new_messages.extend("  " + message for message in lhs_result.messages)
        new_messages.extend("  " + message for message in rhs_result.messages)
        if lhs_result.is_doomed and rhs_result.is_doomed:
            return Diagnostics(
                True,
                new_messages,
                Doomed(visited.engine, visited.columns, new_messages),
            )
        else:
            assert new_relation is not None, "Guaranteed by earlier nested 'if' block."
            return Diagnostics(False, new_messages, new_relation)

    def _visit_unary(self, message: str, visited: UnaryOperation[_T]) -> Diagnostics[_T]:
        new_messages = []
        if (base_result := visited.base.visit(self)).is_doomed:
            new_messages.append(message + " is doomed because:")
        else:
            if base_result.messages:
                new_messages.append(message + " is not doomed, but some contributions were:")
        new_messages.extend("  " + message for message in base_result.messages)
        if base_result.is_doomed:
            new_relation: Relation[_T] = Doomed(visited.engine, visited.columns, new_messages)
        else:
            new_relation = visited.rebase(base_result.relation)
        return Diagnostics(
            is_doomed=base_result.is_doomed,
            messages=new_messages,
            relation=new_relation,
        )
