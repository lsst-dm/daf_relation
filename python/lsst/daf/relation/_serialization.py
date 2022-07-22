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

from . import column_expressions, operations
from ._columns import _T, UniqueKey
from ._exceptions import RelationSerializationError
from ._leaf import Leaf
from ._relation import Identity, Relation, Zero
from ._relation_visitor import RelationVisitor

if TYPE_CHECKING:
    from ._engine import Engine


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
                "name": str(name),
                "engine": engine,
                "columns": columns,
                "unique_keys": unique_keys,
                "parameters": parameters,
            }:
                if not is_str_mapping(parameters):
                    raise RelationSerializationError(
                        f"Expected only string keys for serialized leaf relation, got {parameters}."
                    )
                return Leaf(
                    name,
                    self.read_engine(engine),
                    self.read_columns(columns),
                    self.read_unique_keys(unique_keys),
                    dict(parameters),
                )
            case {"type": "identity", "engine": engine}:
                return Identity(self.read_engine(engine))
            case {"type": "materialization", "base": base, "name": str(name)}:
                return operations.Materialization(self.read_relation(base), name=name)
            case {"type": "join", "lhs": lhs, "rhs": rhs, "condition": condition}:
                return operations.Join(
                    lhs=self.read_relation(lhs),
                    rhs=self.read_relation(rhs),
                    condition=self._read_raw_join_condition(condition),
                )
            case {"type": "projection", "base": base, "columns": columns}:
                return operations.Projection(
                    self.read_relation(base),
                    columns=frozenset(self.read_columns(columns)),
                )
            case {"type": "selection", "base": base, "predicate": predicate}:
                return operations.Selection(
                    self.read_relation(base),
                    self.read_predicate(predicate),
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
            case {"type": "zero", "engine": engine, "columns": columns}:
                return Zero(self.read_engine(engine), self.read_columns(columns))
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
    def read_engine(self, serialized: Any) -> Engine:
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

    def read_expression(self, serialized: Any) -> column_expressions.Expression[_T]:
        """Read a column expression.

        Parameters
        ----------
        serialized
            Serialized form of a column expression.

        Returns
        -------
        expression : `column_expressions.Expression`
            Deserialized column expression.
        """
        match serialized:
            case {"type": "literal", "value": value}:
                return column_expressions.Literal(value)
            case {"type": "reference", "tag": tag}:
                return column_expressions.Reference(tag)
            case {"type": "function", "name": str(name), "args": args}:
                return column_expressions.Function(
                    name,
                    tuple(
                        self.read_expression(arg)
                        for arg in self._iter(args, "Expected a sequence of column expressions, got {}.")
                    ),
                )
            case _:
                raise RelationSerializationError(
                    f"Expected mapping representing a column expression, got {serialized!r}."
                )

    def read_predicate(self, serialized: Any) -> column_expressions.Predicate[_T]:
        """Read a predicate.

        Parameters
        ----------
        serialized
            Serialized form of a predicate.

        Returns
        -------
        expression : `column_expressions.Predicate`
            Deserialized predicate.
        """
        match serialized:
            case {"type": "predicate_literal", "value": bool(value)}:
                return column_expressions.PredicateLiteral(value)
            case {"type": "predicate_reference", "tag": tag}:
                return column_expressions.PredicateReference(tag)
            case {"type": "predicate_function", "name": str(name), "args": args}:
                return column_expressions.PredicateFunction(
                    name,
                    tuple(
                        self.read_expression(arg)
                        for arg in self._iter(args, "Expected a sequence of column expressions, got {}.")
                    ),
                )
            case {"type": "logical_not", "base": base}:
                return column_expressions.LogicalNot(self.read_predicate(base))
            case {"type": "logical_and", "operands": operands}:
                return column_expressions.LogicalAnd(
                    tuple(
                        self.read_predicate(operand)
                        for operand in self._iter(
                            operands, "Expected a sequence of column predicates, got {}."
                        )
                    ),
                )
            case {"type": "logical_or", "operands": operands}:
                return column_expressions.LogicalOr(
                    tuple(
                        self.read_predicate(operand)
                        for operand in self._iter(
                            operands, "Expected a sequence of column predicates, got {}."
                        )
                    ),
                )
            case _:
                raise RelationSerializationError(
                    f"Expected mapping representing a predicate, got {serialized!r}."
                )

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
            raise RelationSerializationError(message.format(raw))
        try:
            return iter(raw)
        except TypeError:
            raise RelationSerializationError(message.format(raw)) from None

    def _read_raw_join_condition(self, raw: Any) -> column_expressions.JoinCondition[_T]:
        match raw:
            case {"predicate": predicate, "lhs_columns": lhs_columns, "rhs_columns": rhs_columns}:
                if predicate is not None:
                    predicate = self.read_predicate(predicate)
                return column_expressions.JoinCondition(
                    predicate,
                    lhs_columns=self.read_columns(lhs_columns),
                    rhs_columns=self.read_columns(rhs_columns),
                )
            case _:
                raise RelationSerializationError(
                    f"Expecting mapping representing a JoinCondition, got {raw!r}."
                )

    def _read_raw_order_by(self, raw: Any) -> tuple[column_expressions.OrderByTerm[_T], ...]:
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
        result: list[column_expressions.OrderByTerm[_T]] = []
        for mapping in self._iter(
            raw, f"Expected an iterable of serialized OrderByTerm mappings, got {raw!r}."
        ):
            match mapping:
                case {"expression": expression, "ascending": bool(ascending)}:
                    result.append(column_expressions.OrderByTerm(self.read_expression(expression), ascending))
                case _:
                    raise RelationSerializationError(
                        f"Expecting mapping representing an OrderByTerm, got {mapping!r}."
                    )
        return tuple(result)


class DictWriter(
    RelationVisitor[_T, dict[str, Any]],
    column_expressions.ExpressionVisitor[_T, dict[str, Any]],
    column_expressions.PredicateVisitor[_T, dict[str, Any]],
):
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
            "name": visited.name,
            "columns": self.write_column_set(visited.columns),
            "engine": self.write_engine(visited.engine),
            "unique_keys": self.write_unique_keys(visited.unique_keys),
        }

    def visit_materialization(self, visited: operations.Materialization[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "materialization",
            "base": visited.base.visit(self),
            "name": visited.name,
        }

    def visit_join(self, visited: operations.Join[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "join",
            "lhs": visited.lhs.visit(self),
            "rhs": visited.rhs.visit(self),
            "condition": {
                "predicate": (
                    None if visited.condition.predicate is None else visited.condition.predicate.visit(self)
                ),
                "lhs_columns": self.write_column_set(visited.condition.lhs_columns),
                "rhs_columns": self.write_column_set(visited.condition.rhs_columns),
            },
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
            "predicate": visited.predicate.visit(self),
        }

    def visit_slice(self, visited: operations.Slice[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "type": "slice",
            "base": visited.base.visit(self),
            "order_by": [
                {"expression": o.expression.visit(self), "ascending": o.ascending} for o in visited.order_by
            ],
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

    def visit_literal(self, visited: column_expressions.Literal[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "literal", "value": visited.value}

    def visit_reference(self, visited: column_expressions.Reference[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "reference", "tag": self.write_column(visited.tag)}

    def visit_function(self, visited: column_expressions.Function[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "function", "name": visited.name, "args": [arg.visit(self) for arg in visited.args]}

    def visit_predicate_literal(self, visited: column_expressions.PredicateLiteral[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "predicate_literal", "value": visited.value}

    def visit_predicate_reference(self, visited: column_expressions.PredicateReference[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "predicate_reference", "tag": visited.tag}

    def visit_predicate_function(self, visited: column_expressions.PredicateFunction[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "predicate_function", "name": str, "args": [arg.visit(self) for arg in visited.args]}

    def visit_logical_not(self, visited: column_expressions.LogicalNot[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "logical_not", "base": visited.base.visit(self)}

    def visit_logical_and(self, visited: column_expressions.LogicalAnd[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "logical_and", "operands": [operand.visit(self) for operand in visited.operands]}

    def visit_logical_or(self, visited: column_expressions.LogicalOr[_T]) -> dict[str, Any]:
        # Docstring inherited.
        return {"type": "logical_or", "operands": [operand.visit(self) for operand in visited.operands]}

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

    def write_engine(self, engine: Engine) -> Any:
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
