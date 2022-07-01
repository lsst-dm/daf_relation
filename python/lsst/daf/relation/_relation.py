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

import json
from abc import abstractmethod
from collections.abc import Iterable, Set
from typing import TYPE_CHECKING, Generic, TypeVar

from ._columns import _T, UniqueKey

if TYPE_CHECKING:
    from ._engines import EngineTag, EngineTree
    from ._join_condition import JoinCondition
    from ._order_by_term import OrderByTerm
    from ._predicate import Predicate
    from ._relation_visitor import _U, RelationVisitor


_S = TypeVar("_S", bound="Relation")


class Relation(Generic[_T]):
    """An abstract interface for conceptual sets of tuples.

    Relation is generic over the type used to represent its columns; see
    `.ColumnTag` for more information.

    Notes
    -----
    `Relation` is an unusual abstract base class in that inheritance from it is
    closed to the `Leaf` class (and its subclasses) and the types in the
    `operations` subpackage; while other external derived classes are not
    explicitly prohibited (there's no graceful way to do that in Python), much
    of the functionality of this package relies on the set of derived types
    enumerated in the `RelationVisitor` interface.  Essentially, instead of the
    types of relations in a tree being extensible, this package treats things
    one can *do* with a relation tree as its primary extension interface.

    **Relation construction**

    The `Relation` base class provides factory methods that should generally be
    used to construct derived instances (instead of the class constructors).
    These factories perform checking and simplifification that can't be always
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

    @staticmethod
    def make_unit(engine: EngineTag) -> Relation[_T]:
        """Construct a relation with no columns and (conceptually one row).

        This relation is the identity for the join operation: joining any
        relation to the unit relation yields the original relation.

        Parameters
        ----------
        engine : `EngineTag`
            Identifier for the engine this relation belongs to.

        Returns
        -------
        relation : `Relation`
            Relation with no columns and a single row.

        See Also
        --------
        operations.Join
        """
        from .operations import Join

        return Join[_T](engine).assert_checked_and_simplified(recursive=False)

    @staticmethod
    def make_zero(
        engine: EngineTag,
        columns: Set[_T],
        doomed_by: Set[str] = frozenset(),
    ) -> Relation[_T]:
        """Construct a relation with no rows.

        This relation is the identity for the union operation: the union of
        any relation to the zero relation yields the original relation.

        Parameters
        ----------
        engine : `EngineTag`
            Identifier for the engine this relation belongs to.
        columns : `~collections.abc.Set` [ `.ColumnTag` ]
            Set of columns for this relation.
        doomed_by : `~collections.abc.Set` [ `str` ]
            Diagnostic messages that can be used to report why the relation
            has no rows.

        Returns
        -------
        relation : `Relation`
            Relation with no rows.

        See Also
        --------
        operations.Union
        """
        from .operations import Union

        # There are no rows, so the unique keys are maximal: each column is
        # itself a unique constraint.
        unique_keys = {frozenset([c]) for c in columns}

        return Union(engine, columns, (), unique_keys, frozenset(doomed_by)).assert_checked_and_simplified(
            recursive=False
        )

    def __repr__(self) -> str:
        from ._serialization import DictWriter

        return json.dumps(self.visit(DictWriter()), indent=2)

    @property
    @abstractmethod
    def engine(self) -> EngineTree:
        """The tree of engines that this relation and those it its built from
        depend on (`EngineTree`).

        The relation's own engine is available as `engine.tag`.
        """
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
    def doomed_by(self) -> Set[str]:
        """A set of diagnostic messages that explain hy this relation has
        no rows (`~collections.abc.Set` [ `str` ]).

        If this set is not empty, the relation has no rows; implementations
        should not return diagnostic messages if the relation merely *may* have
        no rows.
        """
        return frozenset()

    def distinct(self, unique_keys: Set[UniqueKey[_T]] | None = None):
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

        from .operations import Distinct

        return Distinct(self, unique_keys).checked_and_simplified(recursive=False)

    def join(self, *others: Relation[_T], conditions: Iterable[JoinCondition[_T]] = ()) -> Relation[_T]:
        """Construct a relation that performs a natural join operation.

        Parameters
        ----------
        *others : `Relation`
            Relations to join to self.  If any of these is itself a join, and
            the engine sets `EngineOptions.flatten_joins`, the relations and
            conditions will be flattened out into the returned join.  Unit
            relations (joins with no nested relations) are always flattened.
        conditions : `Iterable` [ `JoinCondition` ], optional
            Objects that represent boolean conditions other than equality
            comparison for pairs of column sets.

        Returns
        -------
        relation : `Relation`
            A relation that performs a natural join.  Will be ``self`` or a
            member of ``others`` if all other relation arguments are the unit
            relation.

        Raises
        ------
        EngineError
            Raised if the join includes more than two relations but this is
            not supported by the engine (`EngineOptions.pairwise_joins_only`).
            Also raised if the engines of the relations in the join are not
            consistent, or if a join condition does not support the join's
            engine.
        RelationalAlgebraError
            Raised if a join condition's required columns cannot be satisfied
            by any possible ordering of the join, or if any join condition has
            `~JoinCondition.was_flipped` set to `True`.

        See Also
        --------
        operations.Join
        JoinCondition
        EngineOptions.flatten_joins
        EngineOptions.pairwise_joins_only
        """
        from .operations import Join

        return Join(
            self.engine.tag, (self,) + others, conditions=frozenset(conditions)
        ).checked_and_simplified(recursive=False)

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
        from .operations import Projection

        return Projection(self, frozenset(columns)).checked_and_simplified(recursive=False)

    def selection(self, *predicates: Predicate[_T]) -> Relation[_T]:
        """Construct a relation that filters out rows by applying predicates.

        Parameters
        ----------
        *predicates : `Predicate`
            Objects that represent conceptual functions (not *necessarily*
            Python callables) that are invoked to determine whether each row
            should be included in the result relation.

        Returns
        -------
        relation : `Relation`
            A relation whose rows are filtered according to the given
            ``predicates``.  Will be ``self`` if ``predicates`` is empty.

        Raises
        ------
        ColumnError
            Raised if any predicate's required columns are not in the relation.
        EngineError
            Raised if any predicate does not support the relation's engine.

        See Also
        --------
        operations.Selection
        Predicate
        """
        from .operations import Selection

        return Selection(self, frozenset(predicates)).checked_and_simplified(recursive=False)

    def slice(
        self, order_by: Iterable[OrderByTerm[_T]], offset: int = 0, limit: int | None = None
    ) -> Relation[_T]:
        """Construct a relation that sorts rows and/or filters them based on
        their position in the relation.

        Parameters
        ----------
        order_by : `Iterable` [ `OrderByTerm` ]
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
        from .operations import Slice

        return Slice(self, tuple(order_by), offset, limit).checked_and_simplified(recursive=False)

    def transfer(self, destination: EngineTag) -> Relation[_T]:
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
        from .operations import Transfer

        return Transfer(self, destination).checked_and_simplified(recursive=False)

    def union(self, *others: Relation[_T], unique_keys: Set[UniqueKey[_T]] = frozenset()) -> Relation[_T]:
        """Construct a relation that contains all of the rows from a collection
        of other relations.

        Parameters
        ----------
        *others : `Relation`
            Relations to union with self.  If any of these is itself a union,
            and the engine sets `EngineOptions.flatten_unions`, the relations
            will be flattened out into the returned union.  Zero relations
            (unions with no mested relations) are always flattened.  All
            relations must have the same columns.
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

    @abstractmethod
    def checked_and_simplified(self, *, recursive: bool = True) -> Relation[_T]:
        """Check and simplify this relation, returning one that satisfies both
        the relation derived type's invariants and those of its engine.

        Parameters
        ----------
        recursive : `bool`, optional
            If `True`, descend into nested relations to check and simplify them
            as well.

        Returns
        -------
        relation : `Relation`
            Relation that satisfies all invariants and combines most
            back-to-back operations of the same type into single equivalent
            operations.  Guaranteed to be ``self`` (not just a relation
            equivalent to self) when no simplification is performed).

        Raises
        ------
        ColumnError
            Raised when relation operations are inconsistent in ways that
            involve their columns and/or unique keys.  See the `Relation`
            class factory methods for details.
        RelationalAlgebraError
            Raised when relation operations are inconsistent in ways that do
            not involve their columns and/or unique keys.  See the `Relation`
            class factory methods for details.
        EngineError
            Raised when engines are inconsistent or operations are define in a
            way an engine does not support.
        """
        raise NotImplementedError()

    def assert_checked_and_simplified(self: _S, *, recursive: bool = True) -> _S:
        """Assert that this relation is checked and does not need any
        simplification.

        This simply runs `checked_and_simplified` in an `assert` statement,
        asserting that the result is ``self``.

        Parameters
        ----------
        recursive : `bool`, optional
            If `True`, descend into nested relations to check and simplify them
            as well.

        Returns
        -------
        self : `Relation`
            Always exactly ``self``.

        Raises
        ------
        ColumnError
        RelationalAlgebraError
        EngineError
            See `checked_and_simplified` or relation factory methods.
        AssertionError
            Raised if simplification occurs.
        """
        assert (
            self.checked_and_simplified(recursive=recursive) is self
        ), f"Relation {self} expected to be already checked and simplified."
        return self
