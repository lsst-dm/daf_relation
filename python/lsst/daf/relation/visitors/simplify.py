# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("Simplify",)

from typing import TYPE_CHECKING

from .. import operations
from .._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._join_condition import JoinCondition
    from .._leaf import Leaf
    from .._relation import Relation


class Simplify(RelationVisitor[_T, Relation[_T]]):
    def visit_leaf(self, visited: Leaf[_T]) -> Relation[_T]:
        return visited

    def visit_join(self, visited: operations.Join[_T]) -> Relation[_T]:
        relations_flat: list[Relation[_T]] = []
        conditions_flat: set[JoinCondition[_T]] = set()
        any_changes = False
        for original in visited.relations:
            simplified = original.visit(self)
            any_changes = any_changes or simplified is not original
            match simplified:
                case operations.Join(relations=relations, conditions=conditions):
                    relations_flat.extend(relations)
                    conditions_flat.update(conditions)
                    any_changes = True
                case simplified:
                    relations_flat.append(simplified)
        if len(relations_flat) == 1:
            assert not conditions_flat, "Should be guaranteed by previous Check visitor."
            return relations_flat[0]
        elif not any_changes:
            return visited
        else:
            return operations.Join(visited.engine, tuple(relations_flat), frozenset(conditions_flat))

    def visit_projection(self, visited: operations.Projection[_T]) -> Relation[_T]:
        simplified_base = visited.base.visit(self)
        if visited.columns == visited.base.columns:
            return simplified_base
        match simplified_base:
            case operations.Projection(base=base):
                return operations.Projection(base, visited.columns)
            case _:
                if simplified_base is visited.base:
                    return visited
                return operations.Projection(simplified_base, visited.columns)

    def visit_selection(self, visited: operations.Selection[_T]) -> Relation[_T]:
        simplified_base = visited.base.visit(self)
        if not visited.predicates:
            return simplified_base
        match simplified_base:
            case operations.Selection(base=base, predicates=predicates):
                return operations.Selection(base, predicates | visited.predicates)
            case _:
                if simplified_base is visited.base:
                    return visited
                return operations.Selection(simplified_base, visited.predicates)

    def visit_slice(self, visited: operations.Slice[_T]) -> Relation[_T]:
        simplified_base = visited.base.visit(self)
        match simplified_base:
            case operations.Slice(base=base, order_by=order_by, offset=offset, limit=limit):
                order_by_list = list(order_by)
                order_by_list.extend(visited.order_by)
                combined_offset = visited.offset + offset
                if limit is not None:
                    combined_limit: int | None
                    if visited.limit is not None:
                        original_stop = visited.offset + visited.limit
                        new_stop = offset + limit
                        combined_stop = min(original_stop, new_stop)
                        combined_limit = max(combined_stop - offset, 0)
                    else:
                        combined_limit = visited.limit
                return operations.Slice(base, tuple(order_by_list), combined_offset, combined_limit)
            case _:
                if simplified_base is visited.base:
                    return visited
                return operations.Slice(simplified_base, visited.order_by, visited.offset, visited.limit)

    def visit_transfer(self, visited: operations.Transfer[_T]) -> Relation[_T]:
        simplified_base = visited.base.visit(self)
        if simplified_base.engine == visited.engine:
            return simplified_base
        match simplified_base:
            case operations.Transfer(base=base):
                if base.engine == visited.engine:
                    return base
                return operations.Transfer(base, visited.engine)
            case _:
                if simplified_base is visited.base:
                    return visited
                return operations.Transfer(simplified_base, visited.engine)

    def visit_union(self, visited: operations.Union[_T]) -> Relation[_T]:
        relations_flat: list[Relation[_T]] = []
        extra_doomed_by_flat: set[str] = set()
        any_changes = False
        for original in visited.relations:
            simplified = original.visit(self)
            match simplified:
                case operations.Union(relations=relations, extra_doomed_by=extra_doomed_by):
                    relations_flat.extend(relations)
                    extra_doomed_by_flat.update(extra_doomed_by)
                    any_changes = True
                case _:
                    relations_flat.append(simplified)
                    any_changes = any_changes or simplified is not original
        if len(relations_flat) == 1 and not extra_doomed_by_flat:
            return relations_flat[0]
        elif not any_changes:
            return visited
        else:
            return operations.Union(
                visited.engine,
                visited.columns,
                tuple(relations_flat),
                visited.unique_keys,
                frozenset(extra_doomed_by_flat),
            )
