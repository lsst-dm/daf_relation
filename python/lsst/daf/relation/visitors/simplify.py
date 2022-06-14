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
    from .._leaf_relation import LeafRelation
    from .._relation import Relation


class Simplify(RelationVisitor[_T, Relation[_T]]):
    def visit_leaf(self, visited: LeafRelation[_T]) -> Relation[_T]:
        return visited

    def visit_join(self, visited: operations.JoinRelation[_T]) -> Relation[_T]:
        relations_flat: list[Relation[_T]] = []
        conditions_flat: list[JoinCondition[_T]] = []
        for original in visited.relations:
            match original.visit(self):
                case operations.JoinRelation(relations=relations, conditions=conditions):
                    relations_flat.extend(relations)
                    conditions_flat.extend(conditions)
                case simplified:
                    relations_flat.append(simplified)
        if len(relations_flat) == 1 and not conditions_flat:
            return relations_flat[0]
        else:
            return operations.JoinRelation(visited.engine, tuple(relations_flat), tuple(conditions_flat))

    def visit_projected(self, visited: operations.ProjectedRelation[_T]) -> Relation[_T]:
        simplified_base = visited.base.visit(self)
        if visited.columns == visited.base.columns:
            return simplified_base
        match simplified_base:
            case operations.ProjectedRelation(base=base):
                return operations.ProjectedRelation(base, visited.columns)
            case _:
                if simplified_base is visited.base:
                    return visited
                return operations.ProjectedRelation(simplified_base, visited.columns)

    def visit_selected(self, visited: operations.SelectedRelation[_T]) -> Relation[_T]:
        simplified_base = visited.base.visit(self)
        if not visited.predicates:
            return simplified_base
        match simplified_base:
            case operations.SelectedRelation(base=base, predicates=predicates):
                return operations.SelectedRelation(base, predicates + visited.predicates)
            case _:
                if simplified_base is visited.base:
                    return visited
                return operations.SelectedRelation(simplified_base, visited.predicates)

    def visit_sliced(self, visited: operations.SlicedRelation[_T]) -> Relation[_T]:
        simplified_base = visited.base.visit(self)
        match simplified_base:
            case operations.SlicedRelation(base=base, order_by=order_by, offset=offset, limit=limit):
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
                return operations.SlicedRelation(base, tuple(order_by_list), combined_offset, combined_limit)
            case _:
                if simplified_base is visited.base:
                    return visited
                return operations.SlicedRelation(
                    simplified_base, visited.order_by, visited.offset, visited.limit
                )

    def visit_transfer(self, visited: operations.TransferRelation[_T]) -> Relation[_T]:
        simplified_base = visited.base.visit(self)
        if simplified_base.engine == visited.engine:
            return simplified_base
        match simplified_base:
            case operations.TransferRelation(base=base):
                if base.engine == visited.engine:
                    return base
                return operations.TransferRelation(base, visited.engine)
            case _:
                if simplified_base is visited.base:
                    return visited
                return operations.TransferRelation(simplified_base, visited.engine)

    def visit_union(self, visited: operations.UnionRelation[_T]) -> Relation[_T]:
        relations_flat: list[Relation[_T]] = []
        extra_doomed_by_flat: set[str] = set()
        for original in visited.relations:
            match original.visit(self):
                case operations.UnionRelation(relations=relations, extra_doomed_by=extra_doomed_by):
                    relations_flat.extend(relations)
                    extra_doomed_by_flat.update(extra_doomed_by)
                case simplified:
                    relations_flat.append(simplified)
        if len(relations_flat) == 1 and not extra_doomed_by_flat:
            return relations_flat[0]
        else:
            return operations.UnionRelation(
                visited.engine,
                visited.columns,
                tuple(relations_flat),
                visited.unique_keys,
                frozenset(extra_doomed_by_flat),
            )
