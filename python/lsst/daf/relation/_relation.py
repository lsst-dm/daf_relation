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

__all__ = (
    "Identity",
    "Relation",
    "Zero",
)

import json
from abc import abstractmethod
from collections.abc import Iterable, Set
from typing import TYPE_CHECKING, Generic, TypeVar, final

from lsst.utils.classes import cached_getter, immutable

from . import column_expressions
from ._columns import _T, UniqueKey
from ._exceptions import ColumnError, EngineError

if TYPE_CHECKING:
    from ._engine import Engine
    from ._relation_visitor import _U, RelationVisitor


_S = TypeVar("_S", bound="Relation")


class Relation(Generic[_T]):
    """An abstract interface for conceptual sets of tuples.

    Relation is generic over the type used to represent its columns; see
    `.ColumnTag` for more information.

    Notes
    -----
    `Relation` is an unusual abstract base class in that inheritance from it is
    closed to the `Leaf` class (and its subclasses) the types in the
    `operations` subpackage, and subclasses of the `Extension` class; while
    other external derived classes are not explicitly prohibited (there's no
    graceful way to do that in Python), much of the functionality of this
    package relies on the set of derived types enumerated in the
    `RelationVisitor` interface.  Essentially, instead of the types of
    relations in a tree being arbitrarily extensible, this package treats
    things one can *do* with a relation tree as its primary extension
    interface.

    **Relation construction**

    The `Relation` base class provides factory methods that should generally be
    used to construct derived instances (instead of the class constructors).
    These factories perform checking and simplification that can't be always
    be done in constructors, because they can change the type of the operation
    returned.  The `checked_and_simplified` and `assert_checked_and_simplified`
    methods can be used instead to ensure the invariants of derived types are
    satisfied, and can recursively check and simplify a relation tree.  The
    factory methods on the `Relation` base class do not check and simplify
    recursively; they assume any given operations are already checked and
    simplified.

    Relations should never be modified after construction.

    **String formatting**

    Relations define `repr` to provide a complete (deserializable via
    `serialization.MappingReader`) string representation, and `str` to provide
    a compact but lossy string representation.  The latter should always be
    used in error messages.

    """

    def __repr__(self) -> str:
        from ._serialization import DictWriter

        return json.dumps(self.visit(DictWriter()), indent=2)

    @property
    @abstractmethod
    def engine(self) -> Engine[_T]:
        """The engine that this relation is evaluated by (`Engine`)."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def columns(self) -> Set[_T]:
        """The set of columns for this relation
        (`~collections.abc.Set` [ `.ColumnTag` ])."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        """The set of unique constraints this relation is guaranteed to satisfy
        (`~collections.abc.Set` [ `UniqueKey` ]).

        Each "key" in this set is itself a set of columns.

        Notes
        -----
        When `unique_keys` is empty, rows may not be unique in some engines.
        When it is not empty, rows are guaranteed unique *and* guaranteed to
        remain unique when projected with columns that are a superset of any
        key.  For example, if a relation has columns ``{a, b, c, d}`` and
        unique keys ``{{a}, {b, c}}``, then a projection to columns ``{a, d}``
        will be unique and have unique keys `{{a}}`, and a projection to
        columns ``{b, c, d}`` will be unique with unique keys ``{{b, c}}``,
        while a projection to ``{b, d}`` would not be unique and hence have no
        unique keys.
        """
        raise NotImplementedError()

    @property
    def is_identity(self) -> bool:
        return False

    def calculation(self, tag: _T, expression: column_expressions.Expression[_T]) -> Relation[_T]:
        from . import operations

        return operations.Calculation(self, tag, expression)

    def distinct(self, unique_keys: Set[UniqueKey[_T]] | None = None) -> Relation[_T]:
        """Construct a relation with the same rows and columns as ``self``, but
        with rows guaranteed to be unique.

        Parameters
        ----------
        unique_keys : `~collections.abc.Set` [ `UniqueKey` ], optional
            Set of sets that represent multi-column unique constraints for the
            relation (see the `unique_keys` property for details).  If not
            provided and ``self`` does not already have unique rows, it is
            assumed that all columns together are the only unique constraint,
            i.e. ``{UniqueKey(self.columns)}``.

        Returns
        -------
        relation : `Relation`
            A relation with unique rows.  May be ``self`` if its rows are
            already unique.

        Raises
        ------
        ColumnError
            Raised if ``unique_keys`` includes columns that are not part of the
            relation.
        RelationalAlgebraError
            Raised if ``unique_keys`` is given but is empty, or if ``self``
            already has unique row, but with different keys than those that
            were given.

        See Also
        --------
        operations.Distinct
        """
        if unique_keys is None:
            unique_keys = self.unique_keys if self.unique_keys else {UniqueKey(self.columns)}
        if unique_keys == self.unique_keys:
            return self
        from .operations import Distinct

        return Distinct(self, unique_keys)

    def join(
        self,
        rhs: Relation[_T],
        predicate: column_expressions.Predicate[_T] | None = None,
    ) -> Relation[_T]:
        """Construct a relation that performs a natural join operation.

        Parameters
        ----------
        rhs : `Relation`
            Right-hand side operand in the join.
        predicate : `column_expressions.Predicate`, optional
            Explicit condition that must be satisfied by returned join rows,
            in addition to automatic equality constraints on common columns.

        Returns
        -------
        relation : `Relation`
            A relation that performs a natural join.

        See Also
        --------
        operations.Join
        column_expressions.Predicate
        """
        if predicate is not None and not predicate.is_supported_by(rhs.engine):
            raise EngineError(f"Join predicate {predicate} does not support engine {rhs.engine}.")
        condition = column_expressions.JoinCondition.build(predicate, self.columns, rhs.columns)
        if (result := self._try_join(rhs, condition)) is not None:
            return result
        raise EngineError(
            f"Inconsistent engines in join; {self.engine} != {rhs.engine} "
            f"and insertion before transfer to {self.engine} was not possible."
        )

    def _try_join(
        self, rhs: Relation[_T], condition: column_expressions.JoinCondition[_T]
    ) -> Relation[_T] | None:
        """Attempt to join a new relation to this one, recursing into base
        relations when necessary and possible.

        Parameters
        ----------
        rhs : `Relation`
            Right-hand side operand in the join.
        condition : `column_expressions.JoinCondition`
            Explicit condition that must be satisfied by returned join rows,
            including automatic common columns equality constraints and an
            optional custom predicate.

        Returns
        -------
        join : `Relation` or `None`
            Relation that implements the join, or `None` if the join could not
            be inserted.

        Notes
        -----
        Implementations should only recurse to insert the join into a base
        relation when doing so does not change the definition of the join (e.g.
        the set of common columns that are used in equality constraints), and
        when this relation's engine is not the same as the ``rhs`` engine's.
        They may assume (and callers must guarantee) that the column and join
        condition criteria are satisfied for ``self`` (but not necessarily any
        nested relation), while this method is responsible for checking for
        engine consistency.
        """
        if self.is_identity:
            return rhs
        if rhs.is_identity:
            return self
        if self.engine != rhs.engine:
            return None

        from .operations import Join

        return Join(self, rhs, condition)

    def projection(self, columns: Set[_T]) -> Relation[_T]:
        """Construct a relation whose columns are a subset of this relation's.

        Parameters
        ----------
        columns : `~collections.abc.Set` [ `.ColumnTag` ]
            Columns for the new relation.

        Returns
        -------
        relation : `Relation`
            A relation whose columns are exactly those given.  Will be ``self``
            if ``columns == self.columns``.

        Raises
        ------
        ColumnError
            Raised if ``columns`` is not a subset of ``self.columns``.

        See Also
        --------
        operations.Projection
        """
        if columns == self.columns:
            return self

        from .operations import Projection

        return Projection(self, frozenset(columns))

    def selection(
        self,
        predicate: column_expressions.Predicate[_T],
    ) -> Relation[_T]:
        """Construct a relation that filters out rows by applying predicates.

        Parameters
        ----------
        predicate : `column_expressions.Predicate`
            Object that represents a conceptual functions (not necessarily
            a Python callable) that is invoked to determine whether each row
            should be included in the result relation.

        Returns
        -------
        relation : `Relation`
            A relation whose rows are filtered according to the given
            ``predicate``.

        Raises
        ------
        ColumnError
            Raised if the predicate's required columns are not in the relation.
        EngineError
            Raised if the predicate does not support the relation's engine.

        See Also
        --------
        operations.Selection
        Predicate
        """
        if not predicate.columns_required <= self.columns:
            raise ColumnError(
                f"Predicate {predicate} for base relation {self} needs "
                f"columns {set(predicate.columns_required) - self.columns}."
            )
        if (result := self._try_selection(predicate)) is not None:
            return result
        raise EngineError(f"Predicate {predicate} does not support engine {self.engine}.")

    def _try_selection(self, predicate: column_expressions.Predicate[_T]) -> Relation[_T] | None:
        if not predicate.is_supported_by(self.engine):
            return None
        from .operations import Selection

        return Selection(self, predicate)

    def slice(
        self,
        order_by: Iterable[column_expressions.OrderByTerm[_T]],
        offset: int = 0,
        limit: int | None = None,
    ) -> Relation[_T]:
        """Construct a relation that sorts rows and/or filters them based on
        their position in the relation.

        Parameters
        ----------
        order_by : `Iterable` [ `column_expressions.OrderByTerm` ]
            Iterable of objects that specify a sort order.
        offset : `int`, optional
            Starting index for returned rows, with ``0`` as the first row.
        limit : `int` or `None`, optional
            Maximum number of rows returned, or `None` (default) for no limit.

        Returns
        -------
        relation : `Relation`
            A relation that sorts rows and/or filters them based on their
            position.

        Raises
        ------
        ColumnError
            Raised if an `OrderByTerm` requires columns that are not in the
            relation.
        EngineError
            Raised if ``order_by`` is not empty but this engine does not
            support sorting (`EngineOptions.can_sort` is `False`), or if an
            `OrderByTerm` does not support this engine.

        Notes
        -----
        Ordering and slicing are combined into a single operation because
        strict relational algebra does not involve ordered rows at all, and
        without a sort order positional slicing is of relatively little use
        (with "are there at least N rows" tests a notable exception).  Engines
        are permitted to not support sorting at all (`EngineOptions.can_sort`),
        and are generally not expected to propagate sort order through other
        operations.

        Engines may also provide ways to evaluate relations while sorting
        and/or slicing the final result, without actually representing those
        operations as a relation, which is usually preferable when supported.
        The provided `sql` engine does this.  The provided native `iteration`
        engine does not, but since its final result is just a Python collection
        of mappings, Python builtins like `sorted` and sequence slicing can be
        used instead.

        See Also
        --------
        operations.Slice
        OrderByTerm
        EngineOptions.can_sort
        """
        if not order_by and not offset and limit is None:
            return self

        from .operations import Slice

        return Slice(self, tuple(order_by), offset, limit)

    def transfer(self, destination: Engine) -> Relation[_T]:
        """Construct a relation that represents transferring rows from one
        engine to another.

        Parameters
        ----------
        destination : `EngineTag`
            Tag for the engine of the returned relation.

        Returns
        -------
        relation : `Relation`
            Relation in the given ``destination`` engine.  Will be ``self`` if
            it is already in that engine.  If ``self`` is already a transfer,
            the returned relation will transfer directly from the original
            engine to the given ``destination``; back-to-back transfers are
            always flattened out.

        See Also
        --------
        operations.Transfer
        """
        if destination == self.engine:
            return self

        from .operations import Transfer

        return Transfer(self, destination)

    def union(self, second: Relation[_T], unique_keys: Set[UniqueKey[_T]] = frozenset()) -> Relation[_T]:
        """Construct a relation that contains all of the rows from a collection
        of other relations.

        Parameters
        ----------
        TODO
        unique_keys : `~collections.abc.Set` [ `UniqueKey` ], optional
            Set of sets that represent multi-column unique constraints that
            will be *naturally* satisfied by this union, even if the engine
            does not take any extra action to remove duplicates.  If not
            provided or empty, the returned relation does not guarantee
            uniqueness.

        Returns
        -------
        relation : `Relation`
            A relation with all of the rows of any of the given relations.
            Will be ``self`` or a member of ``others`` if all other relation
            arguments are zero relations.

        Raises
        ------
        ColumnError
            Raised if relations do not all have the same columns, or if any
            relation's unique keys do not cover the given ``unique_keys``
            (see `is_unique_key_covered`), indicating that that relations are
            already non-unique in a way that is inconsistent with the union's
            rows being unique.
        EngineError
            Raised if relations are not all in the same engine, or if the
            number of member relations is greater than two and the engine
            does not support this (`EngineOptions.pairwise_unions_only`).

        Notes
        -----
        This union operation corresponds SQL's ``UNION ALL``, because it does
        not force unique rows in the result (at most it can be told when
        unique rows naturally occur).  Use ``union(...).distinct(...)`` for an
        operation that maps to SQL's ``UNION``.

        See Also
        --------
        operations.Union
        EngineOptions.flatten_unions
        EngineOptions.pairwise_unions_only
        """
        if self.engine == second.engine:
            from .operations import Union

            return Union(self, second, unique_keys)
        else:
            raise NotImplementedError("TODO")

    @abstractmethod
    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        """Call the appropriate method of a visitor object for this relation
        derived type.

        Parameters
        ----------
        visitor : `RelationVisitor`
            Visitor object whose method should be called.

        Returns
        -------
        object
            Defined by ``visitor``.
        """
        raise NotImplementedError()


@final
@immutable
class Identity(Relation[_T]):
    """A leaf `Relation` with one row and no columns.

    Joining any relation to this relation yields the original relation.

    Parameters
    ----------
    engine : `.EngineTag`
        Engine that evaluates this relation.
    """

    def __init__(self, engine: Engine):
        self._engine = engine

    def __str__(self) -> str:
        return "I"

    @property
    def engine(self) -> Engine:
        # Docstring inherited.
        return self._engine

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return frozenset()

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return {frozenset()}

    @property
    def is_identity(self) -> bool:
        # Docstring inherited.
        return True

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_identity(self)


@final
@immutable
class Zero(Relation[_T]):
    """A leaf `Relation` with no rows.

    Joining any relation to the null relation yields the null relation.
    The union of the null relation with any other relation is the other
    relation.

    Parameters
    ----------
    engine : `.EngineTag`
        Engine that evaluates this relation.
    columns : `~collections.abc.Set` [ `.ColumnTag` ]
        Set of columns for this relation.
    """

    def __init__(self, engine: Engine, columns: Set[_T]):
        self._engine = engine
        self._columns = columns

    def __str__(self) -> str:
        return "∅"

    @property
    def engine(self) -> Engine:
        # Docstring inherited.
        return self._engine

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self._columns

    @property  # type: ignore
    @cached_getter
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return {frozenset()}

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_zero(self)
