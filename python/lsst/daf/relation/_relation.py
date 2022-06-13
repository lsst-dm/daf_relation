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

__all__ = ("Relation",)

from abc import abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Generic, Iterable

from ._exceptions import MismatchedUnionError, MissingColumnError

if TYPE_CHECKING:
    from ._bounds import _B
    from ._column_tag import _T
    from ._join_condition import JoinCondition
    from ._order_by_term import OrderByTerm
    from ._predicate import Predicate
    from ._relation_visitor import _U, RelationVisitor


class Relation(Generic[_T, _B]):
    @staticmethod
    def make_unit(
        bounds: _B,
    ) -> Relation[_T, _B]:
        from .leaves import UnitRelation

        return UnitRelation(bounds)

    @staticmethod
    def make_zero(
        columns: AbstractSet[_T],
        bounds: _B,
        doomed_by: AbstractSet[str] = frozenset(),
        connections: AbstractSet[frozenset[_T]] = frozenset(),
    ) -> Relation[_T, _B]:
        from .leaves import ZeroRelation

        return ZeroRelation(columns, bounds, doomed_by, connections)

    @property
    @abstractmethod
    def columns(self) -> AbstractSet[_T]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def bounds(self) -> _B:
        raise NotImplementedError()

    @property
    @abstractmethod
    def connections(self) -> AbstractSet[frozenset[_T]]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_full(self) -> bool:
        raise NotImplementedError()

    @property
    @abstractmethod
    def unique_keys(self) -> AbstractSet[frozenset[_T]]:
        raise NotImplementedError()

    @property
    def is_materialized(self) -> bool:
        return False

    @property
    def doomed_by(self) -> AbstractSet[str]:
        return frozenset()

    def forced_unique(self, keys: AbstractSet[frozenset[_T]]) -> Relation[_T, _B]:
        if not self.unique_keys:
            from .operations import ForcedUniqueRelation

            return ForcedUniqueRelation(self, keys)
        else:
            return self

    def join(
        self,
        *others: Relation[_T, _B],
        conditions: Iterable[JoinCondition[_T]] = (),
        extra_connections: Iterable[frozenset[_T]] = (),
    ) -> Relation[_T, _B]:
        # If an argument's `_flatten_joins` yields no terms (e.g. a unit
        # relation), we usually want to leave it out of a join, or if there's
        # only one other argument, we want to just return the other argument.
        # But if there are only unit arguments, we want to return that unit
        # argument.
        fallback = self
        relations = list(self._flatten_join_relations())
        conditions = list(conditions)
        extra_connections = list(extra_connections)
        for other in others:
            new_relations = list(other._flatten_join_relations())
            if new_relations:
                fallback = other
            relations.extend(new_relations)
            conditions.extend(other._flatten_join_conditions())
            extra_connections.extend(other._flatten_join_extra_connections())
        # TODO: There is currently no checking on whether each given join
        # condition matches a pair of relations, or whether we have either an
        # explicit join condition or a common-tags condition to connect all
        # relations.  How to approach this depends on whether we want to save
        # those matches in the relation for user later by visitors
        if len(relations) < 2:
            if conditions:
                raise RuntimeError("Cannot add join conditions with only one relation.")
            return fallback
        from .operations import JoinRelation

        return JoinRelation(tuple(relations), conditions, extra_connections)

    def projected(self, columns: AbstractSet[_T]) -> Relation[_T, _B]:
        if columns != self.columns:
            from .operations import ProjectedRelation

            return ProjectedRelation(self, columns)
        else:
            return self

    def selected(self, *predicates: Predicate[_T, _B]) -> Relation[_T, _B]:
        if predicates:
            from .operations import SelectedRelation

            for p in predicates:
                if not p.columns_required <= self.columns:
                    raise MissingColumnError(
                        f"Predicate {p} needs columns {set(p.columns_required - self.columns)}."
                    )
            return SelectedRelation(self, predicates)
        else:
            return self

    def sliced(
        self, order_by: Iterable[OrderByTerm[_T]], offset: int = 0, limit: int | None = None
    ) -> Relation[_T, _B]:
        order_by = tuple(order_by)
        # TypeError may seem strange below, but it's what Python usually raises
        # when you pass an invalid combination of arguments to a function.
        if not order_by:
            raise TypeError(
                "Cannot slice an unordered relation; to obtain an arbitrary "
                "set of result rows from an unordered relation, pass offset "
                "and/or limit to_sql_executable when executing it."
            )
        if not offset and limit is None:
            raise TypeError(
                "Cannot order a relation unless it is being sliced with "
                "nontrivial offset and/or limit; to obtain ordered rows from "
                "a relation, pass order_by to to_sql_executable when "
                "executing it."
            )

        for t in order_by:
            if not t.columns_required <= self.columns:
                raise MissingColumnError(
                    f"OrderByTerm {t} needs columns {set(t.columns_required - self.columns)}."
                )
        from .operations import SlicedRelation

        return SlicedRelation(self, order_by, offset, limit)

    def union(
        self, *others: Relation[_T, _B], unique_keys: AbstractSet[frozenset[_T]] = frozenset()
    ) -> Relation[_T, _B]:
        # See `join` for what this fallback logic does; in this case it's any
        # zero relation that plays the role of the unit relation, and the
        # doomed_by messages that play the role of the join conditions.
        fallback = self
        relations = list(self._flatten_union_relations())
        extra_doomed_by = set(self._flatten_union_doomed_by())
        columns = self.columns
        for other in others:
            new_relations = list(other._flatten_union_relations())
            if new_relations:
                fallback = other
            for r in new_relations:
                if columns != r.columns:
                    raise MismatchedUnionError(
                        f"Mismatched columns in union: {set(columns)} != {set(r.columns)}."
                    )
            relations.extend(new_relations)
            extra_doomed_by.update(other._flatten_union_doomed_by())
        if len(relations) < 2:
            return fallback.forced_unique(unique_keys)
        from .operations import UnionRelation

        return UnionRelation(tuple(relations), unique_keys, extra_doomed_by)

    def _flatten_join_relations(self) -> Iterable[Relation[_T, _B]]:
        return (self,)

    def _flatten_join_conditions(self) -> Iterable[JoinCondition[_T]]:
        return ()

    def _flatten_join_extra_connections(self) -> Iterable[frozenset[_T]]:
        return ()

    def _flatten_union_relations(self) -> Iterable[Relation[_T, _B]]:
        if not self.doomed_by:
            return (self,)
        else:
            return ()

    def _flatten_union_doomed_by(self) -> Iterable[str]:
        if not self.doomed_by:
            return frozenset()
        else:
            return self.doomed_by

    @abstractmethod
    def visit(self, visitor: RelationVisitor[_T, _B, _U]) -> _U:
        raise NotImplementedError()
