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

__all__ = ("PushPredicates",)

import dataclasses
from typing import TYPE_CHECKING, AbstractSet, Generic

from .. import operations
from .._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._leaf import Leaf
    from .._predicate import Predicate
    from .._relation import Relation


@dataclasses.dataclass
class _Result(Generic[_T]):
    relation: Relation[_T]
    matched: AbstractSet[Predicate[_T]]

    @classmethod
    def finish(cls, base: Relation[_T], candidates: AbstractSet[Predicate[_T]]) -> _Result:
        matched = {p for p in candidates if base.engine in p.state and base.columns >= p.columns_required}
        if matched:
            return cls(operations.Selection(base, frozenset(matched)), matched)
        else:
            return cls(base, frozenset())


class PushPredicates(RelationVisitor[_T, _Result[_T]]):
    def __init__(self, predicates: AbstractSet[Predicate[_T]], *, until_single_engine: bool):
        self.predicates = predicates
        self.until_single_engine = until_single_engine

    def visit_leaf(self, visited: Leaf[_T]) -> _Result[_T]:
        return _Result.finish(visited, self.predicates)

    def visit_join(self, visited: operations.Join[_T]) -> _Result[_T]:
        if self.until_single_engine and visited.engine.depth == 1:
            return _Result.finish(visited, self.predicates)
        # We can push a predicate past a join if we can push it into any of its
        # members, but it's also fine to push the same predicate into multiple
        # members (and usually a good idea, unless it's a particularly
        # expensive one to evaluate).  Here we chose to push each predicate to
        # all members that are willing to accept it.
        remaining_predicates = set(self.predicates)
        new_relations: list[Relation[_T]] = []
        for relation in visited.relations:
            r = relation.visit(self)
            remaining_predicates.difference_update(r.matched)
            new_relations.append(r.relation)
        if remaining_predicates == self.predicates:
            base = visited
        else:
            base = operations.Join(visited.engine, tuple(new_relations), visited.conditions)
        # It is possible we can apply some predicates to the join that we could
        # not apply to individual members, because the join has more columns.
        # We *could* even split up the join and do the selection on some
        # sub-join, but chose not to because we prefer to limit the tree depth
        # to keep associative/commutative symmetries more obvious.
        return _Result.finish(base, remaining_predicates)

    def visit_projection(self, visited: operations.Projection[_T]) -> _Result[_T]:
        if self.until_single_engine and visited.engine.depth == 1:
            return _Result.finish(visited, self.predicates)
        # We can always push predicates past a projection if the projection's
        # base can handle them, because the projection always inherits its
        # base's engine tree and it can only remove columns.
        r = visited.base.visit(self)
        return _Result(operations.Projection(r.relation, visited.columns), self.predicates - r.matched)

    def visit_selection(self, visited: operations.Selection[_T]) -> _Result[_T]:
        if self.until_single_engine and visited.engine.depth == 1:
            return _Result.finish(visited, self.predicates)
        # We can always push new predicates past a selection if the selection's
        # base can handle them, because the selection always inherits its
        # base's engine tree (and we require here that the tree already have
        # any transfers inserted, so a selection's own predicates are always
        # compatible with its engine).
        r = visited.base.visit(self)
        match r.relation:
            case operations.Selection(base=nested_base, predicates=nested_predicates):
                # Avoid back-to-back selections by merging them.
                base = operations.Selection(nested_base, nested_predicates | visited.predicates)
            case _:
                base = operations.Selection(r.relation, visited.predicates)
        return _Result(base, r.matched)

    def visit_slice(self, visited: operations.Slice[_T]) -> _Result[_T]:
        # We can never push predicates past a slice, so no recursion; just see
        # if we can wrap visited in a selection.
        return _Result.finish(visited, self.predicates)

    def visit_transfer(self, visited: operations.Transfer[_T]) -> _Result[_T]:
        if self.until_single_engine and visited.engine.depth == 1:
            return _Result.finish(visited, self.predicates)
        # We may be able to push predicates past a transfer if they support
        # the source engine.  There may be others we can handle with an outer
        # selection after the transfer, and some we can't handle at all.
        r = visited.base.visit(self)
        return _Result.finish(
            operations.Transfer(r.relation, visited.engine.tag), self.predicates - r.matched
        )

    def visit_union(self, visited: operations.Union[_T]) -> _Result[_T]:
        if self.until_single_engine and visited.engine.depth == 1:
            return _Result.finish(visited, self.predicates)
        unmatched_in_any: set[Predicate[_T]] = set()
        new_relations: list[Relation[_T]] = []
        for relation in visited.relations:
            r = relation.visit(self)
            unmatched_in_any.update(self.predicates - r.matched)
            new_relations.append(r.relation)
        matched = self.predicates - unmatched_in_any
        if unmatched_in_any:
            # Some relations didn't accept all predicates.  Need to try agai,
            # and this time just pass the set they can all accept.
            new_visitor = PushPredicates(matched, until_single_engine=self.until_single_engine)
            new_relations.clear()
            for relation in visited.relations:
                r = relation.visit(new_visitor)
                assert r.matched == new_visitor.predicates, "All given predicates should now be matched."
                new_relations.append(r.relation)
        # A union is always in the same engine as its members (we assume
        # transfers have been inserted), and it has the same columns, so
        # there's no way we could handle unmatched predicates at this point
        # via the outer selection _Result.finish would add.
        return _Result(
            operations.Union(
                visited.engine,
                visited.columns,
                tuple(new_relations),
                visited.unique_keys,
                visited.extra_doomed_by,
            ),
            matched,
        )
