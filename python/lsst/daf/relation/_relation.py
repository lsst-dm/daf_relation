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
from typing import TYPE_CHECKING, AbstractSet, Generic, Iterable, TypeVar

from ._columns import _T, UniqueKey
from ._exceptions import ColumnError

if TYPE_CHECKING:
    from ._engines import EngineTag, EngineTree
    from ._join_condition import JoinCondition
    from ._order_by_term import OrderByTerm
    from ._predicate import Predicate
    from ._relation_visitor import _U, RelationVisitor


_S = TypeVar("_S", bound="Relation")


class Relation(Generic[_T]):
    @staticmethod
    def make_unit(engine: EngineTag) -> Relation[_T]:
        from .operations import Join

        return Join[_T](engine).assert_checked_and_simplified(recursive=False)

    @staticmethod
    def make_zero(
        engine: EngineTag,
        columns: AbstractSet[_T],
        unique_keys: AbstractSet[UniqueKey[_T]] = frozenset(),
        doomed_by: AbstractSet[str] = frozenset(),
        **kwargs: bool,
    ) -> Relation[_T]:
        from .operations import Union

        return Union(engine, columns, (), unique_keys, frozenset(doomed_by)).checked_and_simplified(
            recursive=False
        )

    @property
    @abstractmethod
    def engine(self) -> EngineTree:
        raise NotImplementedError()

    @property
    @abstractmethod
    def columns(self) -> AbstractSet[_T]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def unique_keys(self) -> AbstractSet[UniqueKey[_T]]:
        raise NotImplementedError()

    @property
    def doomed_by(self) -> AbstractSet[str]:
        return frozenset()

    def distinct(self, unique_keys: AbstractSet[UniqueKey[_T]] | None = None):
        if unique_keys is None:
            unique_keys = {UniqueKey(self.columns)}

        from .operations import Distinct

        return Distinct(self, unique_keys).checked_and_simplified(recursive=False)

    def join(self, *others: Relation[_T], conditions: Iterable[JoinCondition[_T]] = ()) -> Relation[_T]:
        from .operations import Join

        return Join(
            self.engine.tag, (self,) + others, conditions=frozenset(conditions)
        ).checked_and_simplified(recursive=False)

    def projection(self, columns: AbstractSet[_T]) -> Relation[_T]:
        from .operations import Projection

        return Projection(self, frozenset(columns)).checked_and_simplified(recursive=False)

    def selection(self, *predicates: Predicate[_T]) -> Relation[_T]:
        from .operations import Selection

        return Selection(self, frozenset(predicates)).checked_and_simplified(recursive=False)

    def slice(
        self, order_by: Iterable[OrderByTerm[_T]], offset: int = 0, limit: int | None = None
    ) -> Relation[_T]:
        from .operations import Slice

        return Slice(self, tuple(order_by), offset, limit).checked_and_simplified(recursive=False)

    def transfer(self, engine: EngineTag) -> Relation[_T]:
        from .operations import Transfer

        return Transfer(self, engine).checked_and_simplified(recursive=False)

    def union(
        self, *others: Relation[_T], unique_keys: AbstractSet[UniqueKey[_T]] = frozenset()
    ) -> Relation[_T]:
        from .operations import Union

        return Union(
            self.engine.tag,
            self.columns,
            (self,) + others,
            unique_keys=unique_keys,
            extra_doomed_by=frozenset(),
        ).checked_and_simplified(recursive=False)

    @abstractmethod
    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        raise NotImplementedError()

    @abstractmethod
    def checked_and_simplified(self, *, recursive: bool = True) -> Relation[_T]:
        raise NotImplementedError()

    def assert_checked_and_simplified(self: _S, *, recursive: bool = True) -> _S:
        assert (
            self.checked_and_simplified(recursive=recursive) is self
        ), f"Relation {self} expected to be already checked and simplified."
        return self

    def _check_unique_keys_in_columns(self) -> None:
        for k in self.unique_keys:
            if not k.issubset(self.columns):
                raise ColumnError(
                    f"Unique key {k} for relation {self} involves columns "
                    f"{set(k - self.columns)} not in the relation."
                )
