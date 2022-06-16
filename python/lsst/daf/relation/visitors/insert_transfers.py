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

__all__ = ("InsertTransfers",)

import dataclasses
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, TypeVar, cast

from .. import operations
from .._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._engine_tag import EngineTag
    from .._join_condition import JoinCondition
    from .._leaf import Leaf
    from .._relation import Relation


_V = TypeVar("_V")


@dataclasses.dataclass(slots=True)
class EngineMergeTree:
    sources: dict[EngineTag, EngineMergeTree]
    destination: EngineTag

    def find(
        self, destination: EngineTag, initial: _V, recurse: Callable[[EngineMergeTree], _V | None]
    ) -> _V | None:
        if self.destination == destination:
            # Found it, start unrolling the recursion.
            return initial
        else:
            # Recurse until we find the given destination.
            for source_tree in self.sources.values():
                if (result := recurse(source_tree)) is not None:
                    # Found our destination downstream, keep unrolling the
                    # recursion.
                    return result
            # This branch is a dead-end.
            return None


class InsertTransfers(RelationVisitor[_T, Relation[_T]]):
    def __init__(self, paths: EngineMergeTree):
        self.paths = paths

    def visit_leaf(self, visited: Leaf[_T]) -> Relation[_T]:
        return visited

    def visit_join(self, visited: operations.Join[_T]) -> Relation[_T]:
        # Recurse into relations and group them by engine, while tracking
        # changes to see if we can shortcut to return the original.
        no_changes: bool = True
        relations_by_engine: defaultdict[EngineTag, list[Relation[_T]]] = defaultdict(list)
        for relation in visited.relations:
            processed_relation = relation.visit(self)
            no_changes = (
                no_changes and processed_relation is relation and processed_relation.engine == visited.engine
            )
            relations_by_engine[processed_relation.engine].append(processed_relation)
        if no_changes and all(visited.engine in c.state for c in visited.conditions):
            return visited
        # Make a dict of conditions so we can remove those we've handled as we
        # go.
        conditions_to_do = {condition for condition in visited.conditions}

        def traverse(tree: EngineMergeTree) -> Relation[_T] | None:
            # Relations that were part of the (recursively processed) original
            # join with the destination engine.
            destination_relations = relations_by_engine.pop(tree.destination, [])
            # Relations with other engines that should be combined via the
            # destination engine if there are more than one, or passed through
            # as-is if there is only one.
            source_relations: list[Relation[_T]] = [
                source_relation
                for source_tree in tree.sources.values()
                if (source_relation := traverse(source_tree)) is not None
            ]
            if len(destination_relations) + len(source_relations) > 1:
                # We have to do a join at this level; first transfer all source
                # relations to the destination engine.  While it's possible
                # some of these will have the same source engine, the path tree
                # has instructed us not to join them in that engine, and
                # instead transfer them all to the destination engine and then
                # join.
                destination_relations.extend(
                    operations.Transfer(base, tree.destination) for base in source_relations
                )
                # Identify join conditions that can operate within this engine
                # and the relations now in that engine.
                matching_conditions: set[JoinCondition[_T]] = {
                    condition
                    for condition in conditions_to_do
                    if tree.destination in condition.state and condition.match(destination_relations)
                }
                conditions_to_do.difference_update(matching_conditions)
                return operations.Join(
                    tree.destination, tuple(destination_relations), frozenset(matching_conditions)
                )
            elif destination_relations:
                # Only one relation, and it's already in the destination
                # engine.
                return destination_relations[0]
            elif source_relations:
                # Only one relation, and it's in some other engine.  That's
                # fine; we don't want to transfer to the destination engine
                # unless there's something to do in that engine.
                return source_relations[0]
            else:
                return None

        # Actually run the traversal, and cast away the possibility of None; at
        # this outermost level, we could only get None if the original Join was
        # empty, but that should always follow the shortcut path up top.
        result = cast(Relation[_T], traverse(self.paths))
        # It is possible for some relations to have been dropped here if the
        # path tree didn't include their engines.
        if relations_by_engine:
            raise RuntimeError(
                f"Engines for relations {set(relations_by_engine)} in join not covered by path tree."
            )
        # Same check for join conditions.
        if conditions_to_do:
            raise RuntimeError(f"No path tree engines matching join conditions {conditions_to_do}.")
        return result

    def visit_projection(self, visited: operations.Projection[_T]) -> Relation[_T]:
        # A Projection can happen in any engine, so we just need to recursively
        # visit the base relation.
        processed_base = visited.base.visit(self)
        if processed_base is visited.base:
            return visited
        else:
            return operations.Projection(processed_base, visited.columns)

    def visit_selection(self, visited: operations.Selection[_T]) -> Relation[_T]:
        processed_base = visited.base.visit(self)
        todo = {predicate for predicate in visited.predicates if processed_base.engine not in predicate.state}
        if processed_base is visited.base and not todo:
            return visited

        def traverse(tree: EngineMergeTree) -> Relation[_T] | None:
            # Recurse until we find processed_base.engine in the tree, or fail
            # to.
            if (base := tree.find(processed_base.engine, processed_base, traverse)) is None:
                return None
            # Look for predicates that support the current (destination)
            # engine.
            matching = {predicate for predicate in todo if tree.destination in predicate.state}
            if matching:
                todo.difference_update(matching)
                if base.engine != tree.destination:
                    base = operations.Transfer(base, tree.destination)
                return operations.Selection(base, frozenset(matching))
            else:
                return base

        # Actually run the traversal and check that we succeeded.
        result = traverse(self.paths)
        if result is None:
            raise RuntimeError(f"Engine path tree does not include selection base {processed_base}.")
        if todo:
            raise RuntimeError(
                f"Engine path tree does not allow predicates {todo} to be applied to {result}."
            )
        return result

    def visit_slice(self, visited: operations.Slice[_T]) -> Relation[_T]:
        processed_base = visited.base.visit(self)
        supported_engines: set[EngineTag] = {visited.order_by[0].state.keys()}
        for term in visited.order_by[1:]:
            supported_engines.intersection_update(term.state.keys())
        if processed_base is visited.base and processed_base.engine in supported_engines:
            return visited

        def traverse(tree: EngineMergeTree) -> tuple[Relation[_T], bool] | None:
            # Recurse until we find processed_base.engine in the tree, or fail
            # to.
            if (base_and_done := tree.find(processed_base.engine, (processed_base, False), traverse)) is None:
                return None
            # Did we already find the slice at at another level?  If so, just
            # keep unwinding the recursion.
            base, done = base_and_done
            if done:
                return base, True
            # See if we can apply all of the order_by terms in this engine.
            if tree.destination in supported_engines:
                if base.engine != tree.destination:
                    base = operations.Transfer(base, tree.destination)
                return (
                    operations.Slice(base, visited.order_by, offset=visited.offset, limit=visited.limit),
                    True,
                )
            return base, False

        # Actually run the traversal and check that we succeeded.
        if (result_and_done := traverse(self.paths)) is None:
            raise RuntimeError(f"Engine path tree does not include slice base {processed_base}.")
        result, done = result_and_done
        if not done:
            if not supported_engines:
                raise RuntimeError(f"Order-by terms {visited.order_by} have no supported engines in common.")
            else:
                raise RuntimeError(
                    f"Engine path tree does not allow order-by terms {visited.order_by} "
                    f"with supported engines {supported_engines} to be applied to {result}."
                )
        return result

    def visit_transfer(self, visited: operations.Transfer[_T]) -> Relation[_T]:
        return visited

    def visit_union(self, visited: operations.Union[_T]) -> Relation[_T]:
        # Recurse into relations and group them by engine, while tracking
        # changes to see if we can shortcut to return the original.
        no_changes: bool = True
        relations_by_engine: defaultdict[EngineTag, list[Relation[_T]]] = defaultdict(list)
        for relation in visited.relations:
            processed_relation = relation.visit(self)
            no_changes = (
                no_changes and processed_relation is relation and processed_relation.engine == visited.engine
            )
            relations_by_engine[processed_relation.engine].append(processed_relation)
        if no_changes:
            return visited

        def traverse(tree: EngineMergeTree) -> Relation[_T] | None:
            # Relations that were part of the (recursively processed) original
            # union with the destination engine.
            destination_relations = relations_by_engine.pop(tree.destination, [])
            # Relations with other engines that should be combined via the
            # destination engine if there are more than one, or passed through
            # as-is if there is only one.
            source_relations: list[Relation[_T]] = [
                source_relation
                for source_tree in tree.sources.values()
                if (source_relation := traverse(source_tree)) is not None
            ]
            if len(destination_relations) + len(source_relations) > 1:
                # We have to do a union at this level; first transfer all
                # source relations to the destination engine.  While it's
                # possible some of these will have the same source engine, the
                # path tree has instructed us not to union them in that engine,
                # and instead transfer them all to the destination engine and
                # then union.
                destination_relations.extend(
                    operations.Transfer(base, tree.destination) for base in source_relations
                )
                return operations.Union(
                    tree.destination,
                    visited.columns,
                    tuple(destination_relations),
                    unique_keys=visited.unique_keys,
                    # We pass the extra 'doomed_by' messages in to the unions
                    # at all levels, because we don't know where they go and
                    # they'll be deduped by virtue of being in a set whenever
                    # we report them.
                    extra_doomed_by=visited.extra_doomed_by,
                )
            elif destination_relations:
                # Only one relation, and it's already in the destination
                # engine.
                return destination_relations[0]
            elif source_relations:
                # Only one relation, and it's in some other engine.  That's
                # fine; we don't want to transfer to the destination engine
                # unless there's something to do in that engine.
                return source_relations[0]
            else:
                return None

        # Actually run the traversal, and cast away the possibility of None; at
        # this outermost level, we could only get None if the original Union
        # was empty, but that should always follow the shortcut path up top.
        result = cast(Relation[_T], traverse(self.paths))
        # It is possible for some relations to have been dropped here if the
        # path tree didn't include their engines.
        if relations_by_engine:
            raise RuntimeError(
                f"Engines for relations {set(relations_by_engine)} in union not covered by path tree."
            )
        return result
