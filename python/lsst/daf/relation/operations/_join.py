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

__all__ = ("Join",)

import itertools
from collections.abc import Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import cached_getter, immutable

from .._columns import _T, UniqueKey
from .._engines import EngineTag, EngineTree
from .._exceptions import EngineError, RelationalAlgebraError
from .._join_condition import JoinCondition
from .._relation import Relation

if TYPE_CHECKING:
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Join(Relation[_T]):
    """An operation `Relation` that performs a natural join.

    A natural join includes all columns from all input relations while keeping
    only rows where all input relations have the same values for any common
    columns.

    Parameters
    ----------
    engine : `EngineTag`
        Engine the join is performed in.  This must be the same as the engine
        of all input relations.
    relations : `tuple` [ `Relation` , ... ]
        Input relations for the join.
    conditions : `frozenset` [ `JoinCondition` ]
        Custom (generally non-equality) conditions on which to join pairs of
        relations.

    Notes
    -----
    Like other operations, `Join` objects should only be constructed directly
    by code that can easily guarantee their `checked_and_simplify` invariants;
    in all other contexts, the `Relation.join` factory should be used instead.

    Join objects with no relations are permitted (with no conditions, either),
    and are used to represent the "unit relation" (see `Relation.make_unit`),
    though these are often simplified out after they are added to larger
    relation trees.  Join objects with one relation are not permitted; these
    should always be simplified out as a no-op.  Some relations may prohibit
    joins with more than two relations (see
    `EngineOptions.pairwise_joins_only`).

    See `Relation.join` for the `checked_and_simplified` behavior for this
    class.
    """

    def __init__(
        self,
        engine: EngineTag,
        relations: tuple[Relation[_T], ...] = (),
        conditions: frozenset[JoinCondition[_T]] = frozenset(),
    ):
        self._engine = engine
        self.relations = relations
        self.conditions = conditions

    relations: tuple[Relation[_T], ...]
    """Input relations for the join (`tuple` [ `Relation`, ... ])."""

    conditions: frozenset[JoinCondition[_T]]
    """Custom (generally non-equality) conditions on which to join pairs of
    relations (`frozenset` [ `JoinCondition` ]).
    """

    def __str__(self) -> str:
        return f"({'⋈ '.join(str(r) for r in self.relations)})"

    @property  # type: ignore
    @cached_getter
    def engine(self) -> EngineTree:
        # Docstring inherited.
        return EngineTree.build_if_needed(self._engine, {r.engine for r in self.relations})

    @property  # type: ignore
    @cached_getter
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        result: set[_T] = set()
        for relation in self.relations:
            result.update(relation.columns)
        return result

    @property  # type: ignore
    @cached_getter
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        current_keys: set[UniqueKey[_T]] = set()
        for relation in self.relations:
            current_keys = {
                key1.union(key2) for key1, key2 in itertools.product(current_keys, relation.unique_keys)
            }
        return current_keys

    @property  # type: ignore
    @cached_getter
    def doomed_by(self) -> Set[str]:
        # Docstring inherited.
        result: set[str] = set()
        for relation in self.relations:
            result.update(relation.doomed_by)
        return result

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_join(self)

    def checked_and_simplified(self, *, recursive: bool = True) -> Relation[_T]:
        # Docstring inherited.
        relations_flat: list[Relation[_T]] = []
        conditions_flat: set[JoinCondition[_T]] = set()
        any_changes = False
        for condition in self.conditions:
            if condition.was_flipped:
                raise RelationalAlgebraError(
                    f"Join condition {condition} was flipped; only unflipped "
                    "conditions should be added to relations."
                )
            if self.engine not in condition.engine_state:
                raise EngineError(
                    f"Join condition {condition} supports engine(s) {set(condition.engine_state.keys())}, "
                    f"while join has {self.engine}."
                )
        for original in self.relations:
            if recursive:
                relation = original.checked_and_simplified(recursive=True)
                if relation is not original:
                    any_changes = True
            else:
                relation = original
            match relation:
                case Join(relations=nested_relations, conditions=nested_conditions):
                    if not nested_relations:
                        any_changes = True
                    elif self.engine.tag.options.flatten_joins:
                        relations_flat.extend(nested_relations)
                        conditions_flat.update(nested_conditions)
                        any_changes = True
                    else:
                        relations_flat.append(relation)
        conditions_to_match = set(conditions_flat)
        for relation in relations_flat:
            columns_in_others = set(itertools.chain(r.columns for r in self.relations if r is not relation))
            conditions_to_match.difference_update(
                JoinCondition.find_matching(relation.columns, columns_in_others, conditions_to_match)
            )
            if relation.engine.tag != self.engine.tag:
                raise EngineError(
                    f"Join member {relation} has engine {relation.engine.tag}, "
                    f"while join has {self.engine.tag}."
                )
        if conditions_to_match:
            raise RelationalAlgebraError(f"No join order matches join condition(s) {conditions_to_match}.")
        if len(relations_flat) == 1:
            assert not conditions_flat, "Should be guaranteed by previous check on matching conditions."
            return relations_flat[0]
        if self.engine.tag.options.pairwise_joins_only:
            if len(relations_flat) > 2:
                raise EngineError(f"Engine {self.engine.tag} requires pairwise joins only.")
        if not any_changes:
            return self
        else:
            return Join(self.engine.tag, tuple(relations_flat), frozenset(conditions_flat))
