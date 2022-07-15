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
from collections.abc import Iterator, Set
from typing import TYPE_CHECKING, Any, Generic, cast

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
    """An abstract base class for deserializing `Relation` trees from nested
    mappings of builtin types.

    Notes
    -----
    This base class provides a complete implementation of the main entry point
    for deserialization, `read_relation`.  That delegates to seven abstract
    methods that must be implemented by derived classes:

    - `read_columns`
    - `read_engine`
    - `read_leaf`
    - `read_predicate`
    - `read_join_condition`
    - `read_order_by_term`

    See Also
    --------
    DictWriter
    """

    def read_relation(self, mapping: Any) -> Relation[_T]:
        """Read a relation that has been serialized as a mapping.

        Parameters
        ----------
        mapping : `Mapping`
            A mapping with string keys that corresponds to a serialized
            relation, typically produced by a `DictWriter` instance (and often
            converted to/from JSON or YAML).  This is annotated as `typing.Any`
            because this method takes responsibility for checking that it is
            a mapping (with recognized keys).

        Returns
        -------
        relation : `Relation`
            Deserialized relation.

        Raises
        ------
        RelationSerializationError
            Raised if the given mapping is not a correctly serialized relation.
        """
        match mapping:
            case {"type": "distinct", "base": base, "unique_keys": unique_keys}:
                return operations.Distinct(
                    self.read_relation(base), self.read_unique_keys(unique_keys)
                ).checked_and_simplified(recursive=False)
            case {
                "type": "leaf",
                "engine": engine,
                "columns": columns,
                "unique_keys": unique_keys,
                **extra,
            }:
                return self.read_leaf(
                    engine=self.read_engine(engine),
                    columns=self.read_columns(columns),
                    unique_keys=self.read_unique_keys(unique_keys),
                    extra=cast(dict[str, Any], extra),
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
    def read_columns(self, serialized: Any) -> Set[_T]:
        """Read a set of column tags.

        Parameters
        ----------
        serialized
            Serialized form of a set of columns.  The default `DictWriter`
            implementation writes this as a `list` of `str`, but custom
            reader/writers pairs are free to use any type compatible with
            their serialization format.

        Returns
        -------
        columns : `~collections.abc.Set` [ `.ColumnTag` ]
            Set of column tag objects.
        """
        raise NotImplementedError()

    @abstractmethod
    def read_engine(self, serialized: Any) -> EngineTag:
        """Read an engine tag.

        Parameters
        ----------
        serialized
            Serialized form of an engine tag.  The default `DictWriter`
            implementation writes this as a `str`, but custom reader/writers
            pairs are free to use any type compatible with their serialization
            format.

        Returns
        -------
        engine : `EngineTag`
            Set of column tag objects.
        """
        raise NotImplementedError()

    def read_unique_keys(self, serialized: Any) -> Set[UniqueKey[_T]]:
        """Read a relation's unique keys.

        Parameters
        ----------
        serialized
            Serialized form of a unique constraint.  The default `DictWriter`
            implementation writes this as a `list`, with each element the type
            it uses for a set of columns (by default a `list` of `str`).
            Custom reader/writers pairs are free to use any type compatible
            with their serialization format.

        Returns
        -------
        columns : `~collections.abc.Set` [ `UniqueKey` ]
            Set of unique keys.

        Notes
        -----
        This method delegates to `read_columns` and should rarely need to be
        overridden itself.
        """
        return {UniqueKey(self.read_columns(k)) for k in serialized}

    @abstractmethod
    def read_leaf(
        self,
        engine: EngineTag,
        columns: Set[_T],
        unique_keys: Set[UniqueKey[_T]],
        extra: dict[str, Any],
    ) -> Leaf[_T]:
        """Read a leaf relation.

        Parameters
        ----------
        name : `str`
            Name of the leaf relation.
        engine : `EngineTag`
            Engine tag for the relation.  Implementations may change this as
            long as the resulting relation tree still has consistent engines
            throughout.
        columns : `~collections.abc.Set` [ `.ColumnTag` ]
            Set of columns for the relation.
        unique_keys : `~collections.abc.Set` [ `UniqueKey` ]
            Set of sets representing unique constraints.
        extra : `dict`
            Dictionary of extra engine-dependent state.

        Returns
        -------
        leaf : `Leaf`
            Leaf relation.
        """
        raise NotImplementedError()

    @abstractmethod
    def read_join_condition(
        self,
        columns_required: tuple[frozenset[_T], frozenset[_T]],
        engines_supported: Set[EngineTag],
        extra: dict[str, Any],
    ) -> JoinCondition[_T]:
        """Read a join condition.

        Parameters
        ----------
        columns_required : `tuple` [ `~collections.abc.Set`, \
                `~collections.abc.Set` ]
            A 2-element `tuple` of column sets, indicating the columns this
            condition joins.
        engines_suported : `~collections.abc.Set` [ `EngineTag` ]
            Set of engines this condition is expected to support.
            Implementations may drop or otherwise modify the supported engines
            as long as the resulting relation tree still has consistent engines
            throughout.
        extra : `dict`
            Dictionary of subclass-specific state.

        Returns
        -------
        condition : `JoinCondition`
            Join condition with `~JoinCondition.engine_state` set to an empty
            `dict`.  Derived classes will typically delegate to `super`, update
            the engine-specific state themselves, and return the result.
        """
        raise NotImplementedError()

    @abstractmethod
    def read_predicate(
        self,
        columns_required: frozenset[_T],
        engines_supported: Set[EngineTag],
        extra: dict[str, Any],
    ) -> Predicate[_T]:
        """Read a predicate.

        Parameters
        ----------.
        columns_required : `~collections.abc.Set` [ `.ColumnTag` ]
            Set of columns required to evaluate the predicate.
        engines : `~collections.abc.Set` [ `EngineTag` ]
            Set of engines this predicate is expected to support.
            Implementations may drop or otherwise modify the supported engines
            as long as the resulting relation tree still has consistent engines
            throughout.
         extra : `dict`
            Dictionary of subclass-specific state.


        Returns
        -------
        predicate : `Predicate`
            Predicate with `~Predicate.engine_state` set to an empty `dict`.
            Derived classes will typically delegate to `super`, update the
            engine-specific state themselves, and return the result.
        """
        raise NotImplementedError()

    @abstractmethod
    def read_order_by_term(
        self,
        columns_required: frozenset[_T],
        engines_supported: Set[EngineTag],
        ascending: bool,
        extra: dict[str, Any],
    ) -> OrderByTerm[_T]:
        """Read an order-by term.

        Parameters
        ----------
        name : `str`
            Name of the order by term.
        columns_required : `~collections.abc.Set` [ `.ColumnTag` ]
            Set of columns required to evaluate the order-by term.
        engines_supported : `~collections.abc.Set` [ `EngineTag` ]
            Set of engines this order-by term is expected to support.
            Implementations may drop or otherwise modify the supported engines
            as long as the resulting relation tree still has consistent engines
            throughout.
        ascending : `bool`
            Whether the term should be sorted in ascending order.
        extra : `dict`
            Dictionary of subclass-specific state.

        Returns
        -------
        order_by_term : `OrderByTerm`
            Order-by term with `~Predicate.engine_state` set to an empty
            `dict`.  Derived classes will typically delegate to `super`, update
            the engine-specific state themselves, and return the result.
        """
        raise NotImplementedError()

    def _iter(self, raw: Any, message: str) -> Iterator[Any]:
        """Attempt to iterate over the given non-`str` object, handling any
        errors that occur.

        Parameters
        ----------
        raw
            Object to attempt to iterate over.
        message : `str`
            Message to use in the exception raised if it is a `str` or is not
            iterable.

        Returns
        -------
        iterator : `Iterator`
            An iterator over ``raw``.

        Raises
        ------
        RelationSerializationError
            Raised if ``raw`` is a ``str`` instance or if it is not iterable.
        """
        if isinstance(raw, str):
            raise RelationSerializationError(message)
        try:
            return iter(raw)
        except TypeError:
            raise RelationSerializationError(message) from None

    def _read_raw_join_conditions(self, raw: Any) -> frozenset[JoinCondition[_T]]:
        """Read a set of `JoinCondition` instances.

        This method delegates to `read_join_condition` and should not be
        overridden or even called by derived classes.

        Parameters
        ----------
        raw
            Iterable expected to contain serialized `JoinCondition` objects.

        Returns
        -------
        conditions : `frozenset` [ `JoinCondition` ]
            Set of `JoinCondition`objects.
        """
        result: set[JoinCondition[_T]] = set()
        for mapping in self._iter(
            raw, f"Expected an iterable of serialized JoinCondition mappings, got {raw!r}."
        ):
            match mapping:
                case {
                    "name": str(name),
                    "columns_required": [cr0, cr1],
                    "engines": engines,
                    **general_state,
                }:
                    result.add(  # type: ignore
                        self.read_join_condition(
                            name,
                            (frozenset(self.read_columns(cr0)), frozenset(self.read_columns(cr1))),
                            cast(dict[str, Any], general_state),
                            {self.read_engine(e) for e in engines},
                        )
                    )
                case _:
                    raise RelationSerializationError(
                        f"Expecting mapping representing a JoinCondition, got {mapping!r}."
                    )
        return frozenset(result)

    def _read_raw_predicates(self, raw: Any) -> tuple[Predicate[_T], ...]:
        """Read a set of `Predicate` instances.

        This method delegates to `read_predicate` and should not be
        overridden or even called by derived classes.

        Parameters
        ----------
        raw
            Iterable expected to contain serialized `Predicate` objects.

        Returns
        -------
        predicates : `tuple` [ `Predicate`, ... ]
            Set of `Predicate`objects.
        """
        result: list[Predicate[_T]] = []
        for mapping in self._iter(
            raw, f"Expected an iterable of serialized Predicate mappings, got {raw!r}."
        ):
            match mapping:
                case {
                    "columns_required": columns_required,
                    "engines_supported": engines,
                    **extra,
                }:
                    result.append(
                        self.read_predicate(
                            frozenset(self.read_columns(columns_required)),
                            {self.read_engine(e) for e in engines},
                            cast(dict[str, Any], extra),
                        )
                    )
                case _:
                    raise RelationSerializationError(
                        f"Expecting mapping representing a Predicate, got {mapping!r}."
                    )
        return tuple(result)

    def _read_raw_order_by(self, raw: Any) -> tuple[OrderByTerm[_T], ...]:
        """Read a set of `OrderByTerm` instances.

        This method delegates to `read_order_by_term` and should not be
        overridden or even called by derived classes.

        Parameters
        ----------
        raw
            Iterable expected to contain serialized `OrderByTerm` objects.

        Returns
        -------
        order_by : `tuple` [ `OrderByTerm`, ... ]
            Tuple of `OrderByTerm`objects.
        """
        result: list[OrderByTerm[_T]] = []
        for mapping in self._iter(
            raw, f"Expected an iterable of serialized OrderByTerm mappings, got {raw!r}."
        ):
            match mapping:
                case {
                    "columns_required": columns_required,
                    "engines_supported": engines,
                    "ascending": bool(ascending),
                    **extra,
                }:
                    result.append(
                        self.read_order_by_term(
                            frozenset(self.read_columns(columns_required)),
                            {self.read_engine(e) for e in engines},
                            ascending,
                            cast(dict[str, Any], extra),
                        )
                    )
                case _:
                    raise RelationSerializationError(
                        f"Expecting mapping representing a Predicate, got {mapping!r}."
                    )
        return tuple(result)


class DictWriter(RelationVisitor[_T, dict[str, Any]]):
    """A visitor class that transforms a relation tree into a nested dictionary
    suitable for serialization via JSON, YAML, or similar formats.

    Notes
    -----
    Unlike its reading counterpart `MappingReader`, `DictWriter` is a concrete
    class that often won't need to be subclassed, because it can delegate more
    work to the extension types it is serializing:

    - Derived types (from `Leaf`, `Extension`, `Predicate`, `OrderByTerm`, and
      `JoinCondition`) are handled by calling ``serialize`` methods.
    - `.ColumnTag` and `EngineTag` instances are formatted with `str` (which
      can be overridden by reimplementing `write_columns` and `write_engine`).

    `DictWriter` sorts all iterables of `.ColumnTag`, `Relation`,
    `JoinCondition`, `OrderByTerm`, and `Predicate` as it saves them to make
    the serialized form more deterministic, but it does not descend into the
    dictionaries returned by various ``write_extra_to_dict`` methods to sort
    iterables within them.

    See Also
    --------
    MappingReader
    """

    def visit_distinct(self, visited: operations.Distinct[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "distinct",
            "base": visited.base.visit(self),
            "unique_keys": self.write_unique_keys(visited.unique_keys),
        }

    def visit_leaf(self, visited: Leaf[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "leaf",
            **visited.serialize(self),
        }

    def visit_join(self, visited: operations.Join[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "join",
            "engine": self.write_engine(visited.engine),
            "relations": sorted(r.visit(self) for r in visited.relations),
            "conditions": sorted(jc.serialize(self) for jc in visited.conditions),
        }

    def visit_projection(self, visited: operations.Projection[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "projection",
            "base": visited.base.visit(self),
            "columns": self.write_column_set(visited.columns),
        }

    def visit_selection(self, visited: operations.Selection[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "selection",
            "base": visited.base.visit(self),
            "predicates": sorted(p.serialize(self) for p in visited.predicates),
        }

    def visit_slice(self, visited: operations.Slice[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "slice",
            "base": visited.base.visit(self),
            "order_by": [o.serialize(self) for o in visited.order_by],
            "offset": visited.offset,
            "limit": visited.limit,
        }

    def visit_transfer(self, visited: operations.Transfer) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "transfer",
            "base": visited.base.visit(self),
            "destination": self.write_engine(visited.engine),
        }

    def visit_union(self, visited: operations.Union[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "union",
            "engine": self.write_engine(visited.engine),
            "columns": self.write_column_set(visited.columns),
            "relations": sorted(r.visit(self) for r in visited.relations),
            "unique_keys": self.write_unique_keys(visited.unique_keys),
            "extra_doomed_by": sorted(visited.extra_doomed_by),
        }

    def write_column(self, column: _T) -> Any:
        """Convert a single column tag to a serializable type.

        Parameters
        ----------
        column : `.ColumnTag`
            Column tag to save.

        Returns
        -------
        serialization
            Serializable object (`list`, `dict`, `str`, etc.) representing the
            column.  The default implementation returns the `str`
            representation of the column tag.
        """
        return str(column)

    def write_column_set(self, columns: Set[_T]) -> Any:
        """Convert a set of column tags to a serializable type.

        Parameters
        ----------
        columns : `~collections.abc.Set` [ `.ColumnTag` ]
            Set of column tags to save.

        Returns
        -------
        serialization
            Serializable object (`list`, `dict`, `str`, etc.) representing the
            columns.  The default implementation returns a sorted `list` of
            the result of calling `write_column` on each column tag.
        """
        return sorted(self.write_column(t) for t in columns)

    def write_engine(self, engine: EngineTag) -> Any:
        """Convert an engine tag to a serializable type.

        Parameters
        ----------
        engine : `EngineTag`
            Engine tag to save.

        Returns
        -------
        serialization
            Serializable object (`list`, `dict`, `str`, etc.) representing the
            engine tag.  The default implementation returns the `str`
            representation.
        """
        return str(engine)

    def write_unique_keys(self, unique_keys: Set[UniqueKey[_T]]) -> Any:
        """Convert set of unique keys to a serializable type.

        Parameters
        ----------
        unique_keys : `~collections.abc.Set` [ `UniqueKey` ]
            Set of unique keys to save.

        Returns
        -------
        serialization
            Serializable object (`list`, `dict`, `str`, etc.) representing the
            set of keys.  The default implementation calls `write_column_set`
            on each key and returns a sorted list of the results.
        """
        return sorted(self.write_column_set(key) for key in unique_keys)
