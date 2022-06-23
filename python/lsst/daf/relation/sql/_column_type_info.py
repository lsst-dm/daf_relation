# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "ColumnTypeInfo",
    "JoinConditionState",
    "OrderByTermState",
    "PredicateState",
)

from abc import abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Generic, Iterable, Mapping, Protocol, Sequence, TypeVar, cast

import sqlalchemy

from .._columns import _T

if TYPE_CHECKING:
    from .._engines import EngineTag
    from .._join_condition import JoinCondition
    from .._order_by_term import OrderByTerm
    from .._predicate import Predicate


_L = TypeVar("_L")

_L_contra = TypeVar("_L_contra", contravariant=True)


class PredicateState(Protocol[_T, _L_contra]):
    def __call__(
        self, logical_columns: Mapping[_T, _L_contra], columns_required: AbstractSet[_T]
    ) -> sqlalchemy.sql.ColumnElement:
        ...


class OrderByTermState(Protocol[_T, _L_contra]):
    def __call__(
        self, logical_columns: Mapping[_T, _L_contra], columns_required: AbstractSet[_T], ascending: bool
    ) -> sqlalchemy.sql.ColumnElement:
        ...


class JoinConditionState(Protocol[_T, _L_contra]):
    def __call__(
        self,
        logical_columns: tuple[Mapping[_T, _L_contra], Mapping[_T, _L_contra]],
        columns_required: tuple[AbstractSet[_T], AbstractSet[_T]],
        was_flipped: bool,
    ) -> sqlalchemy.sql.ColumnElement:
        ...


class ColumnTypeInfo(Generic[_T, _L]):
    @abstractmethod
    def extract_mapping(
        self, tags: AbstractSet[_T], sql_columns: sqlalchemy.sql.ColumnCollection
    ) -> Mapping[_T, _L]:
        raise NotImplementedError()

    @abstractmethod
    def select_items(
        self, items: Iterable[tuple[_T, _L]], sql_from: sqlalchemy.sql.FromClause
    ) -> sqlalchemy.sql.Select:
        raise NotImplementedError()

    @abstractmethod
    def make_zero_select(self, columns: AbstractSet[_T]) -> sqlalchemy.sql.Select:
        raise NotImplementedError()

    @abstractmethod
    def make_unit_subquery(self) -> sqlalchemy.sql.FromClause:
        raise NotImplementedError()

    def convert_order_by(
        self, engine: EngineTag, order_by: OrderByTerm[_T], logical_columns: Mapping[_T, _L]
    ) -> sqlalchemy.sql.ColumnElement:
        callable: OrderByTermState[_T, _L] | None = order_by.engine_state.get(engine)
        if callable is not None:
            return callable(logical_columns, order_by.columns_required, order_by.ascending)
        elif len(order_by.columns_required) == 1:
            (tag,) = order_by.columns_required
            logical_column = cast(sqlalchemy.sql.ColumnElement, logical_columns[tag])
            return logical_column if order_by.ascending else logical_column.desc()
        else:
            raise NotImplementedError(
                f"No default SQL implementation for OrderByTerm {order_by} "
                f"with no state for engine {engine} and multiple columns required."
            )

    def convert_predicate(
        self, engine: EngineTag, predicate: Predicate[_T], logical_columns: Mapping[_T, _L]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        callable: PredicateState[_T, _L] | None = predicate.engine_state.get(engine)
        if callable is not None:
            return callable(logical_columns, predicate.columns_required)
        else:
            raise NotImplementedError(
                f"No default SQL implementation for Predicate {predicate} "
                f"with no state for engine {engine}."
            )

    def convert_join_condition(
        self,
        engine: EngineTag,
        condition: JoinCondition[_T],
        logical_columns: tuple[Mapping[_T, _L], Mapping[_T, _L]],
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        callable: JoinConditionState[_T, _L] | None = condition.engine_state.get(engine)
        if callable is not None:
            return callable(logical_columns, condition.columns_required, condition.was_flipped)
        else:
            raise NotImplementedError(
                f"No default SQL implementation for Predicate {condition} "
                f"with no state for engine {engine}."
            )
