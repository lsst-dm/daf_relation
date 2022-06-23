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

__all__ = ("MappingReader", "DictWriter")

from abc import abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Any, Dict, Generic, Iterator, cast

from . import operations
from ._columns import _T, UniqueKey
from ._exceptions import RelationSerializationError
from ._join_condition import JoinCondition
from ._leaf import Leaf
from ._order_by_term import OrderByTerm
from ._predicate import Predicate
from ._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from ._engines import EngineTag
    from ._relation import Relation


class MappingReader(Generic[_T]):
    def read_relation(self, mapping: Any) -> Relation[_T]:
        match mapping:
            case {"type": "distinct", "base": base, "unique_keys": unique_keys}:
                return operations.Distinct(
                    self.read_relation(base), self.read_unique_keys(unique_keys)
                ).checked_and_simplified(recursive=False)
            case {
                "type": "leaf",
                "name": str(name),
                "engine": engine,
                "columns": columns,
                "unique_keys": unique_keys,
                **general_state,
            }:
                return self.read_leaf(
                    name,
                    engine=self.read_engine(engine),
                    columns=self.read_columns(columns),
                    unique_keys=self.read_unique_keys(unique_keys),
                    general_state=cast(dict[str, Any], general_state),
                ).checked_and_simplified(recursive=False)
            case {"type": "join", "engine": engine, "relations": relations, "conditions": conditions}:
                return operations.Join(
                    self.read_engine(engine),
                    relations=tuple(
                        self.read_relation(r)
                        for r in self._iter(
                            relations,
                            f"Expected an iterable representing joined relations, got {relations!r}.",
                        )
                    ),
                    conditions=self._read_raw_join_conditions(conditions),
                ).checked_and_simplified(recursive=False)
            case {"type": "projection", "base": base, "columns": columns}:
                return operations.Projection(
                    self.read_relation(base),
                    columns=frozenset(self.read_columns(columns)),
                ).checked_and_simplified(recursive=False)
            case {"type": "selection", "base": base, "predicates": predicates}:
                return operations.Selection(
                    self.read_relation(base),
                    predicates=self._read_raw_predicates(predicates),
                ).checked_and_simplified(recursive=False)
            case {
                "type": "slice",
                "base": base,
                "order_by": order_by,
                "offset": int(offset),
                "limit": int() | None as limit,
            }:
                return operations.Slice(
                    self.read_relation(base),
                    order_by=self._read_raw_order_by(order_by),
                    offset=offset,
                    limit=limit,
                ).checked_and_simplified(recursive=False)
            case {"type": "transfer", "base": base, "destination": destination}:
                return operations.Transfer(
                    self.read_relation(base), destination=self.read_engine(destination)
                ).checked_and_simplified(recursive=False)
            case {
                "type": "union",
                "engine": engine,
                "columns": columns,
                "relations": relations,
                "unique_keys": unique_keys,
                "extra_doomed_by": extra_doomed_by,
            }:
                return operations.Union(
                    self.read_engine(engine),
                    columns=self.read_columns(columns),
                    relations=tuple(
                        self.read_relation(r)
                        for r in self._iter(
                            relations,
                            f"Expected an iterable representing unioned relations, got {relations!r}.",
                        )
                    ),
                    unique_keys=self.read_unique_keys(unique_keys),
                    extra_doomed_by=frozenset(
                        self._iter(
                            extra_doomed_by,
                            f"Expected an iterable representing doom messages, got {extra_doomed_by!r}.",
                        )
                    ),
                ).checked_and_simplified(recursive=False)
            case _:
                raise RelationSerializationError(
                    f"Expected mapping representing a relation, got {mapping!r}."
                )

    @abstractmethod
    def read_columns(self, serialized: Any) -> AbstractSet[_T]:
        raise NotImplementedError()

    @abstractmethod
    def read_engine(self, serialized: Any) -> EngineTag:
        raise NotImplementedError()

    @abstractmethod
    def read_leaf(
        self,
        name: str,
        engine: EngineTag,
        general_state: dict[str, Any],
        columns: AbstractSet[_T],
        unique_keys: AbstractSet[UniqueKey[_T]],
    ) -> Leaf[_T]:
        raise NotImplementedError()

    def read_unique_keys(self, serialized: Any) -> AbstractSet[UniqueKey[_T]]:
        return {frozenset(self.read_columns(k)) for k in serialized}

    def read_join_condition(
        self,
        name: str,
        columns_required: tuple[frozenset[_T], frozenset[_T]],
        general_state: dict[str, Any],
        engines: AbstractSet[EngineTag],
    ) -> JoinCondition[_T]:
        return JoinCondition(name, columns_required, general_state, dict.fromkeys(engines))

    def read_predicate(
        self,
        name: str,
        columns_required: frozenset[_T],
        general_state: dict[str, Any],
        engines: AbstractSet[EngineTag],
    ) -> Predicate[_T]:
        return Predicate(name, columns_required, general_state, dict.fromkeys(engines))

    def read_order_by_term(
        self,
        name: str,
        columns_required: frozenset[_T],
        ascending: bool,
        general_state: dict[str, Any],
        engines: AbstractSet[EngineTag],
    ) -> OrderByTerm[_T]:
        return OrderByTerm(name, columns_required, ascending, general_state, dict.fromkeys(engines))

    def _iter(self, raw: Any, message: str) -> Iterator[Any]:
        if isinstance(raw, str):
            raise RelationSerializationError(message)
        try:
            return iter(raw)
        except TypeError:
            raise RelationSerializationError(message) from None

    def _read_raw_join_conditions(self, raw: Any) -> frozenset[JoinCondition[_T]]:
        result: set[JoinCondition[_T]] = set()
        for mapping in self._iter(
            raw, f"Expected an iterable of serialized JoinCondition mappings, got {raw!r}."
        ):
            match mapping:
                case {"name": str(name), "columns_required": [cr0, cr1], "engines": engines, **general_state}:
                    result.add(
                        self.read_join_condition(
                            name,
                            (frozenset(self.read_columns(cr0)), frozenset(self.read_columns(cr1))),
                            cast(Dict[str, Any], general_state),
                            {self.read_engine(e) for e in engines},
                        )
                    )
                case _:
                    raise RelationSerializationError(
                        f"Expecting mapping representing a JoinCondition, got {mapping!r}."
                    )
        return frozenset(result)

    def _read_raw_predicates(self, raw: Any) -> frozenset[Predicate[_T]]:
        result: set[Predicate[_T]] = set()
        for mapping in self._iter(
            raw, f"Expected an iterable of serialized Predicate mappings, got {raw!r}."
        ):
            match mapping:
                case {
                    "name": str(name),
                    "columns_required": columns_required,
                    "engines": engines,
                    **general_state,
                }:
                    result.add(
                        self.read_predicate(
                            name,
                            frozenset(self.read_columns(columns_required)),
                            cast(Dict[str, Any], general_state),
                            {self.read_engine(e) for e in engines},
                        )
                    )
                case _:
                    raise RelationSerializationError(
                        f"Expecting mapping representing a Predicate, got {mapping!r}."
                    )
        return frozenset(result)

    def _read_raw_order_by(self, raw: Any) -> tuple[OrderByTerm[_T], ...]:
        result: list[OrderByTerm[_T]] = []
        for mapping in self._iter(
            raw, f"Expected an iterable of serialized OrderByTerm mappings, got {raw!r}."
        ):
            match mapping:
                case {
                    "name": str(name),
                    "columns_required": columns_required,
                    "ascending": bool(ascending),
                    "engines": engines,
                    **general_state,
                }:
                    result.append(
                        self.read_order_by_term(
                            name,
                            frozenset(self.read_columns(columns_required)),
                            ascending,
                            cast(Dict[str, Any], general_state),
                            {self.read_engine(e) for e in engines},
                        )
                    )
                case _:
                    raise RelationSerializationError(
                        f"Expecting mapping representing a Predicate, got {mapping!r}."
                    )
        return tuple(result)


class DictWriter(RelationVisitor[_T, Dict[str, Any]]):
    def visit_distinct(self, visited: operations.Distinct[_T]) -> dict[str, Any]:
        return {
            "type": "distinct",
            "base": visited.base.visit(self),
            "unique_keys": self.write_unique_keys(visited.unique_keys),
        }

    def visit_leaf(self, visited: Leaf[_T]) -> dict[str, Any]:
        return {
            "type": "leaf",
            "name": visited.name,
            "engine": self.write_engine(visited.engine.tag),
            "columns": self.write_columns(visited.columns),
            "unique_keys": self.write_unique_keys(visited.unique_keys),
        }

    def visit_join(self, visited: operations.Join[_T]) -> dict[str, Any]:
        return {
            "type": "join",
            "engine": self.write_engine(visited.engine.tag),
            "relations": sorted(r.visit(self) for r in visited.relations),
            "conditions": sorted(
                {
                    "name": jc.name,
                    "columns_required": [self.write_columns(cr) for cr in jc.columns_required],
                    "engines": sorted(self.write_engine(engine) for engine in jc.engine_state),
                    **jc.general_state,
                }
                for jc in visited.conditions
            ),
        }

    def visit_projection(self, visited: operations.Projection[_T]) -> dict[str, Any]:
        return {
            "type": "projection",
            "base": visited.base.visit(self),
            "columns": self.write_columns(visited.columns),
        }

    def visit_selection(self, visited: operations.Selection[_T]) -> dict[str, Any]:
        return {
            "type": "selection",
            "base": visited.base.visit(self),
            "predicates": sorted(
                {
                    "name": p.name,
                    "columns_required": self.write_columns(p.columns_required),
                    "engines": sorted(self.write_engine(engine) for engine in p.engine_state),
                    **p.general_state,
                }
                for p in visited.predicates
            ),
        }

    def visit_slice(self, visited: operations.Slice[_T]) -> dict[str, Any]:
        return {
            "type": "slice",
            "base": visited.base.visit(self),
            "order_by": [
                {
                    "name": o.name,
                    "columns_required": self.write_columns(o.columns_required),
                    "ascending": o.ascending,
                    "engines": sorted(self.write_engine(engine) for engine in o.engine_state),
                    **o.general_state,
                }
                for o in visited.order_by
            ],
            "offset": visited.offset,
            "limit": visited.limit,
        }

    def visit_transfer(self, visited: operations.Transfer) -> dict[str, Any]:
        return {
            "type": "transfer",
            "base": visited.base.visit(self),
            "destination": self.write_engine(visited.engine.tag),
        }

    def visit_union(self, visited: operations.Union[_T]) -> dict[str, Any]:
        return {
            "type": "union",
            "engine": self.write_engine(visited.engine.tag),
            "columns": self.write_columns(visited.columns),
            "relations": sorted(r.visit(self) for r in visited.relations),
            "unique_keys": self.write_unique_keys(visited.unique_keys),
            "extra_doomed_by": sorted(visited.extra_doomed_by),
        }

    def write_columns(self, columns: AbstractSet[_T]) -> Any:
        return sorted(str(t) for t in columns)

    def write_engine(self, engine: EngineTag) -> Any:
        return str(engine)

    def write_unique_keys(self, unique_keys: AbstractSet[UniqueKey[_T]]) -> Any:
        return sorted(self.write_columns(key) for key in unique_keys)
