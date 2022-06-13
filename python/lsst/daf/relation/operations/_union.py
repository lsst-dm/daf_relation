# This file is part of daf_butler.
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

__all__ = ("UnionRelation",)

from typing import TYPE_CHECKING, AbstractSet, Iterable, Iterator, final

from lsst.utils.classes import cached_getter

from .._relation import Relation

if TYPE_CHECKING:
    from .._bounds import _B
    from .._column_tag import _T
    from .._predicate import Predicate
    from .._relation_visitor import _U, RelationVisitor


@final
class UnionRelation(Relation[_T, _B]):
    def __init__(
        self,
        relations: tuple[Relation[_T, _B], ...],
        unique_keys: AbstractSet[frozenset[_T]],
        extra_doomed_by: AbstractSet[str],
    ):
        self._relations = relations
        self._unique_keys = unique_keys
        self._extra_doomed_by = extra_doomed_by

    @property
    def columns(self) -> AbstractSet[_T]:
        return self._relations[0].columns

    @property  # type: ignore
    @cached_getter
    def bounds(self) -> _B:
        return self._relations[0].bounds.union(*[r.bounds for r in self._relations[1:]])

    @property  # type: ignore
    @cached_getter
    def connections(self) -> AbstractSet[frozenset[_T]]:
        first, *rest = self._relations
        result = set(first.connections)
        for relation in rest:
            result.intersection_update(relation.connections)
        return result

    @property  # type: ignore
    @cached_getter
    def is_full(self) -> bool:
        return any(r.is_full for r in self._relations)

    @property
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        return self._unique_keys

    @property  # type: ignore
    @cached_getter
    def doomed_by(self) -> AbstractSet[str]:
        result = set(self._extra_doomed_by)
        for related in self._relations:
            if not related.doomed_by:
                return frozenset()
            result.update(related.doomed_by)
        return result

    def projected(self, columns: AbstractSet[_T]) -> Relation[_T, _B]:
        return Relation.union(*[related.projected(columns) for related in self._relations])

    def selected(self, *predicates: Predicate[_T, _B]) -> Relation[_T, _B]:
        return Relation.union(*[relation.selected(*predicates) for relation in self._relations])

    def _flatten_union_relations(self) -> Iterator[Relation[_T, _B]]:
        return iter(self._relations)

    def _flatten_union_doomed_by(self) -> Iterable[str]:
        result = set(self._extra_doomed_by)
        for relation in self._relations:
            result.update(relation._flatten_union_doomed_by())
        return result

    def visit(self, visitor: RelationVisitor[_T, _B, _U]) -> _U:
        return visitor.visit_union(self, self._relations, self._unique_keys, self._extra_doomed_by)
