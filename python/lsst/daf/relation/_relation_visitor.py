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

__all__ = ("RelationVisitor",)

from abc import abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Any, Generic, Sequence, TypeVar

if TYPE_CHECKING:
    from . import operations
    from ._column_tag import _T
    from ._join_condition import JoinCondition
    from ._leaf_relation import LeafRelation
    from ._order_by_term import OrderByTerm
    from ._predicate import Predicate
    from ._relation import Relation


_U = TypeVar("_U", covariant=True)


class RelationVisitor(Generic[_T, _U]):
    @abstractmethod
    def visit_leaf(
        self,
        visited: LeafRelation[_T],
        state: dict[str, Any],
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_join(
        self,
        visited: operations.JoinRelation[_T],
        relations: Sequence[Relation[_T]],
        conditions: Sequence[JoinCondition[_T]],
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_projected(
        self, visited: operations.ProjectedRelation[_T], base: Relation[_T], columns: AbstractSet[_T]
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_selected(
        self,
        visited: operations.SelectedRelation[_T],
        base: Relation[_T],
        predicates: Sequence[Predicate[_T]],
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_sliced(
        self,
        visited: operations.SlicedRelation[_T],
        base: Relation[_T],
        order_by: Sequence[OrderByTerm[_T]],
        offset: int,
        limit: int | None,
    ) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def visit_union(
        self,
        visited: operations.UnionRelation[_T],
        columns: AbstractSet[_T],
        relations: Sequence[Relation[_T]],
        unique_keys: AbstractSet[frozenset[_T]],
        extra_doomed_by: AbstractSet[str],
    ) -> _U:
        raise NotImplementedError()
