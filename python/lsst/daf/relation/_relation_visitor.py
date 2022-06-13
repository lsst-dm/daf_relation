# This file is part of daf_butler.
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

__all__ = ("RelationVisitor",)

from abc import abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Generic, Sequence, TypeVar

if TYPE_CHECKING:
    from . import leaves, operations
    from ._bounds import _B
    from ._column_tag import _T
    from ._join_condition import JoinCondition
    from ._order_by_term import OrderByTerm
    from ._predicate import Predicate
    from ._relation import Relation


_U = TypeVar("_U", covariant=True)


class RelationVisitor(Generic[_T, _B, _U]):
    @abstractmethod
    def visit_forced_unique(
        self, visited: operations.ForcedUniqueRelation[_T, _B], base: Relation[_T, _B]
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_join(
        self,
        visited: operations.JoinRelation[_T, _B],
        relations: Sequence[Relation[_T, _B]],
        conditions: Sequence[JoinCondition[_T]],
        extra_connections: AbstractSet[frozenset[_T]],
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_projected(
        self, visited: operations.ProjectedRelation[_T, _B], base: Relation[_T, _B], columns: AbstractSet[_T]
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_selected(
        self,
        visited: operations.SelectedRelation[_T, _B],
        base: Relation[_T, _B],
        predicates: Sequence[Predicate[_T, _B]],
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_sliced(
        self,
        visited: operations.SlicedRelation[_T, _B],
        base: Relation[_T, _B],
        order_by: Sequence[OrderByTerm[_T]],
        offset: int,
        limit: int | None,
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_union(
        self,
        visited: operations.UnionRelation[_T, _B],
        relations: Sequence[Relation[_T, _B]],
        extra_doomed_by: AbstractSet[str],
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_unit(self, visited: leaves.UnitRelation[_T, _B], bounds: _B) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_zero(
        self,
        visited: leaves.ZeroRelation[_T, _B],
        columns: AbstractSet[_T],
        bounds: _B,
        doomed_by: AbstractSet[str],
        connections: AbstractSet[frozenset[_T]],
    ) -> _U:
        raise NotImplementedError()
