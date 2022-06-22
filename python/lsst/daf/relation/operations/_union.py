# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the relations of the GNU General Public License as published by
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

__all__ = ("Union",)

from typing import TYPE_CHECKING, AbstractSet, final

from lsst.utils.classes import cached_getter

from .._engines import EngineTag, EngineTree
from .._exceptions import ColumnError, EngineError
from .._relation import Relation

if TYPE_CHECKING:
    from .._column_tag import _T
    from .._relation_visitor import _U, RelationVisitor


@final
class Union(Relation[_T]):
    def __init__(
        self,
        engine: EngineTag,
        columns: AbstractSet[_T],
        relations: tuple[Relation[_T], ...] = (),
        unique_keys: AbstractSet[frozenset[_T]] = frozenset(),
        extra_doomed_by: frozenset[str] = frozenset(),
    ):
        self._engine = engine
        self._columns = columns
        self.relations = relations
        self._unique_keys = unique_keys
        self.extra_doomed_by = extra_doomed_by

    @property  # type: ignore
    @cached_getter
    def engine(self) -> EngineTree:
        return EngineTree.build(self._engine, {r.engine for r in self.relations})

    @property
    def columns(self) -> AbstractSet[_T]:
        return self._columns

    @property
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        return self._unique_keys

    @property  # type: ignore
    @cached_getter
    def doomed_by(self) -> AbstractSet[str]:
        result = set(self.extra_doomed_by)
        for related in self.relations:
            if not related.doomed_by:
                return frozenset()
            result.update(related.doomed_by)
        return result

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        return visitor.visit_union(self)

    def check(self, *, recursive: bool = True) -> None:
        self._check_unique_keys()
        for relation in self.relations:
            if recursive:
                relation.check(recursive=True)
            for key in self.unique_keys:
                if key not in relation.unique_keys and not any(
                    key.issuperset(relation_key) for relation_key in relation.unique_keys
                ):
                    raise ColumnError(
                        f"Union is declared to have unique key {set(key)}, but "
                        f"member {relation} is not unique with those columns."
                    )
            if relation.engine != self.engine:
                raise EngineError(
                    f"Union member {relation} has engine {relation.engine}, "
                    f"while union has {self.engine}."
                )
            if relation.columns != self.columns:
                raise ColumnError(
                    f"Mismatched union columns: {set(relation.columns)} != {set(self.columns)} "
                    f"for relation {relation}."
                )

    def simplified(self, *, recursive: bool = True) -> Relation[_T]:
        relations_flat: list[Relation[_T]] = []
        extra_doomed_by_flat: set[str] = set()
        any_changes = False
        for original in self.relations:
            if recursive:
                simplified = original.simplified(recursive=True)
                any_changes = any_changes or simplified is not original
            else:
                simplified = original
            match simplified:
                case Union(relations=relations, extra_doomed_by=extra_doomed_by):
                    relations_flat.extend(relations)
                    extra_doomed_by_flat.update(extra_doomed_by)
                    any_changes = True
                case _:
                    relations_flat.append(simplified)
        if len(relations_flat) == 1 and not extra_doomed_by_flat:
            return relations_flat[0]
        elif not any_changes:
            return self
        else:
            return Union(
                self.engine.tag,
                self.columns,
                tuple(relations_flat),
                self.unique_keys,
                frozenset(extra_doomed_by_flat),
            )
