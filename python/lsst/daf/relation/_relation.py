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

__all__ = ("Relation",)

from abc import abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Generic, Iterable

if TYPE_CHECKING:
    from ._column_tag import _T
    from ._engine_tag import EngineTag
    from ._join_condition import JoinCondition
    from ._order_by_term import OrderByTerm
    from ._predicate import Predicate
    from ._relation_visitor import _U, RelationVisitor


class Relation(Generic[_T]):
    @staticmethod
    def make_unit(engine: EngineTag) -> Relation[_T]:
        from .operations import Join

        return Join(engine)

    @staticmethod
    def make_zero(
        engine: EngineTag,
        columns: AbstractSet[_T],
        unique_keys: AbstractSet[frozenset[_T]] = frozenset(),
        doomed_by: AbstractSet[str] = frozenset(),
    ) -> Relation[_T]:
        from .operations import Union

        return Union(engine, columns, (), unique_keys, frozenset(doomed_by))

    @property
    @abstractmethod
    def engine(self) -> EngineTag:
        raise NotImplementedError()

    @property
    @abstractmethod
    def columns(self) -> AbstractSet[_T]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        raise NotImplementedError()

    @property
    def doomed_by(self) -> AbstractSet[str]:
        return frozenset()

    def forced_unique(self, keys: AbstractSet[frozenset[_T]]) -> Relation[_T]:
        from .operations import Union

        return Union(self.engine, self.columns, (self,), keys, frozenset())

    def join(
        self,
        *others: Relation[_T],
        conditions: Iterable[JoinCondition[_T]] = (),
    ) -> Relation[_T]:
        from .operations import Join

        return Join(self.engine, (self,) + others, conditions=tuple(conditions))

    def projection(self, columns: AbstractSet[_T]) -> Relation[_T]:
        from .operations import Projection

        return Projection(self, columns)

    def selection(self, *predicates: Predicate[_T]) -> Relation[_T]:
        from .operations import Selection

        return Selection(self, predicates)

    def slice(
        self, order_by: Iterable[OrderByTerm[_T]], offset: int = 0, limit: int | None = None
    ) -> Relation[_T]:
        from .operations import Slice

        return Slice(self, tuple(order_by), offset, limit)

    def union(
        self, *others: Relation[_T], unique_keys: AbstractSet[frozenset[_T]] = frozenset()
    ) -> Relation[_T]:
        from .operations import Union

        return Union(
            self.engine,
            self.columns,
            (self,) + others,
            unique_keys=self.unique_keys,
            extra_doomed_by=frozenset(),
        )

    @abstractmethod
    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        raise NotImplementedError()
