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
from collections.abc import Iterator, Mapping, Set
from typing import TYPE_CHECKING, Any, Generic, TypeGuard

from . import operations
from ._columns import _T, UniqueKey
from ._exceptions import RelationSerializationError
from ._join_condition import JoinCondition
from ._leaf import Leaf
from ._order_by_term import OrderByTerm
from ._predicate import Predicate
from ._relation import Identity, Relation, Zero
from ._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from ._engines import EngineTag


def is_str_mapping(mapping: Any) -> TypeGuard[Mapping[str, Any]]:
    if not isinstance(mapping, Mapping):
        return False
    return all(type(k) is str for k in mapping)


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
                return operations.Distinct(self.read_relation(base), self.read_unique_keys(unique_keys))
            case {
                "type": "leaf",
                **kwargs,
            }:
                if not is_str_mapping(kwargs):
                    raise RelationSerializationError(
                        f"Expected only string keys for serialized leaf relation, got {kwargs}."
                    )
                return self.read_leaf(**kwargs)
            case {"type": "identity", "engine": engine}:
                return Identity(self.read_engine(engine))
            case {"type": "join", "lhs": lhs, "rhs": rhs, "condition": condition}:
                if not is_str_mapping(condition):
                    raise RelationSerializationError(
                        f"Expected mapping with string keys for serialized condition, got {condition}."
                    )
                return operations.Join(
                    lhs=self.read_relation(lhs),
                    rhs=self.read_relation(rhs),
                    condition=self.read_join_condition(**condition) if condition is not None else None,
                )
            case {"type": "zero", "engine": engine, "columns": columns}:
                return Zero(self.read_engine(engine), self.read_columns(columns))
            case {"type": "projection", "base": base, "columns": columns}:
                return operations.Projection(
                    self.read_relation(base),
                    columns=frozenset(self.read_columns(columns)),
                )
            case {"type": "selection", "base": base, "predicate": predicate}:
                if not is_str_mapping(predicate):
                    raise RelationSerializationError(
                        f"Expected mapping with string keys for serialized predicate, got {predicate}."
                    )
                return operations.Selection(
                    self.read_relation(base),
                    self.read_predicate(**predicate),
                )
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
                )
            case {"type": "transfer", "base": base, "destination": destination}:
                return operations.Transfer(
                    self.read_relation(base), destination=self.read_engine(destination)
                )
            case {
                "type": "union",
                "first": first,
                "second": second,
                "unique_keys": unique_keys,
            }:
                return operations.Union(first, second, unique_keys=self.read_unique_keys(unique_keys))
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
    def read_leaf(self, **kwargs: Any) -> Leaf[_T]:
        """Read a leaf relation.

        Parameters
        ----------
        **kwargs
            Keyword arguments derived from deserialized dictionary.

        Returns
        -------
        leaf : `Leaf`
            Leaf relation.
        """
        raise NotImplementedError()

    @abstractmethod
    def read_join_condition(self, **kwargs: Any) -> JoinCondition[_T]:
        """Read a join condition.

        Parameters
        ----------
        **kwargs
            Keyword arguments derived from deserialized dictionary.

        Returns
        -------
        condition : `JoinCondition`
            Join condition.
        """
        raise NotImplementedError()

    @abstractmethod
    def read_predicate(self, **kwargs: Any) -> Predicate[_T]:
        """Read a predicate.

        Parameters
        ----------
        **kwargs
            Keyword arguments derived from deserialized dictionary.

        Returns
        -------
        predicate : `Predicate`
            Predicate.
        """
        raise NotImplementedError()

    @abstractmethod
    def read_order_by_term(self, **kwargs: Any) -> OrderByTerm[_T]:
        """Read an order-by term.

        Parameters
        ----------
        **kwargs
            Keyword arguments derived from deserialized dictionary.

        Returns
        -------
        order_by_term : `OrderByTerm`
            Order-by term.
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
            if not is_str_mapping(mapping):
                raise RelationSerializationError(
                    f"Expecting mapping string keys representing an OrderByTerm, got {mapping!r}."
                )
            result.append(self.read_order_by_term(**mapping))
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

    def visit_identity(self, visited: Identity[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "identity", "engine": self.write_engine(visited.engine)}

    def visit_leaf(self, visited: Leaf[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "leaf",
            **visited.serialize(self),
        }

    def visit_materialization(self, visited: operations.Materialization[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "materialization",
            "base": visited.base.visit(self),
        }

    def visit_join(self, visited: operations.Join[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "join",
            "lhs": visited.lhs.visit(self),
            "rhs": visited.rhs.visit(self),
            "condition": visited.condition.serialize(self) if visited.condition is not None else None,
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
            "predicate": visited.predicate.serialize(self),
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
            "first": visited.first.visit(self),
            "second": visited.second.visit(self),
            "unique_keys": self.write_unique_keys(visited.unique_keys),
        }

    def visit_zero(self, visited: Zero[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "null", "columns": self.write_column_set(visited.columns)}

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
