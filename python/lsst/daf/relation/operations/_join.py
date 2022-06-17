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
from typing import TYPE_CHECKING, AbstractSet, final

from lsst.utils.classes import cached_getter

from .._engines import EngineTag, EngineTree
from .._exceptions import EngineError, RelationalAlgebraError
from .._relation import Relation

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._join_condition import JoinCondition
    from .._relation_visitor import _U, RelationVisitor


@final
class Join(Relation[_T]):
    def __init__(
        self,
        engine: EngineTag,
        relations: tuple[Relation[_T], ...] = (),
        conditions: frozenset[JoinCondition[_T]] = frozenset(),
    ):
        self._engine = engine
        self.relations = relations
        self.conditions = conditions

    @property  # type: ignore
    @cached_getter
    def engine(self) -> EngineTree:
        return EngineTree.build(self._engine, {r.engine for r in self.relations})

    @property  # type: ignore
    @cached_getter
    def columns(self) -> AbstractSet[_T]:
        result: set[_T] = set()
        for relation in self.relations:
            result.update(relation.columns)
        return result

    @property  # type: ignore
    @cached_getter
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        current_keys: set[frozenset[_T]] = set()
        for relation in self.relations:
            current_keys = {
                key1.union(key2) for key1, key2 in itertools.product(current_keys, relation.unique_keys)
            }
        return current_keys

    @property  # type: ignore
    @cached_getter
    def doomed_by(self) -> AbstractSet[str]:
        result: set[str] = set()
        for relation in self.relations:
            result.update(relation.doomed_by)
        return result

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_join(self)

    def check(self, *, recursive: bool = True) -> None:
        for relation in self.relations:
            if relation.engine != self.engine:
                raise EngineError(
                    f"Join member {relation} has engine {relation.engine}, while join has {self.engine}."
                )
            if recursive:
                relation.check()
        for condition in self.conditions:
            if self.engine not in condition.state:
                raise EngineError(
                    f"Join condition {condition} supports engine(s) {set(condition.state.keys())}, "
                    f"while join has {self.engine}."
                )
            if not condition.match(self.relations):
                raise RelationalAlgebraError(f"No match for join condition {condition}.")
