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

__all__ = ("TransferFunction", "TransferVisitor")

from typing import TYPE_CHECKING, Mapping, Protocol

from . import operations
from ._columns import _T
from ._exceptions import EngineError
from ._relation import Relation
from ._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from ._engines import EngineTag
    from ._leaf import Leaf


class TransferFunction(Protocol[_T]):
    def __call__(self, source: Relation[_T]) -> Relation[_T]:
        ...


class TransferVisitor(RelationVisitor[_T, Relation[_T]]):
    def __init__(self, transfer_functions: Mapping[tuple[EngineTag, EngineTag], TransferFunction]):
        self.transfer_functions = transfer_functions

    def visit_distinct(self, visited: operations.Distinct[_T]) -> Relation[_T]:
        if (base := visited.base.visit(self)) is not visited.base:
            return operations.Distinct(base, visited.unique_keys)
        return visited

    def visit_leaf(self, visited: Leaf[_T]) -> Relation[_T]:
        return visited

    def visit_join(self, visited: operations.Join[_T]) -> Relation[_T]:
        relations = []
        any_changed: bool = False
        for original in visited.relations:
            if (relation := original.visit(self)) is not original:
                any_changed = True
            relations.append(relation)
        if any_changed:
            return operations.Join(visited.engine, tuple(relations), visited.conditions)
        return visited

    def visit_projection(self, visited: operations.Projection[_T]) -> Relation[_T]:
        if (base := visited.base.visit(self)) is not visited.base:
            return operations.Projection(base, visited.columns)
        return visited

    def visit_selection(self, visited: operations.Selection[_T]) -> Relation[_T]:
        if (base := visited.base.visit(self)) is not visited.base:
            return operations.Selection(base, visited.predicates)
        return visited

    def visit_slice(self, visited: operations.Slice[_T]) -> Relation[_T]:
        if (base := visited.base.visit(self)) is not visited.base:
            return operations.Slice(base, visited.order_by, visited.offset, visited.limit)
        return visited

    def visit_transfer(self, visited: operations.Transfer) -> Relation[_T]:
        # First traverse the tree down to leaf nodes.
        source = visited.base.visit(self)
        # On the way back down to the root, use the registered functions to
        # execute transfers from one engine to another.
        try:
            transfer_function = self.transfer_functions[source.engine, visited.engine]
        except KeyError:
            raise EngineError(
                f"No function registered for transfer from {source.engine} to {visited.engine}."
            ) from None
        return transfer_function(visited.base)

    def visit_union(self, visited: operations.Union[_T]) -> Relation[_T]:
        relations = []
        any_changed: bool = False
        for original in visited.relations:
            if (relation := original.visit(self)) is not original:
                any_changed = True
            relations.append(relation)
        if any_changed:
            return operations.Union(
                visited.engine,
                visited.columns,
                tuple(relations),
                visited.unique_keys,
            )
        return visited
