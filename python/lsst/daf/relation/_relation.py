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
import json

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
    """An abstract interface for conceptual sets of tuples.

    Relation is generic over the type used to represent its columns; see
    `ColumnTag` for more information.

    Notes
    -----
    The `Relation` class represents the core concept of `relational algebra`_:
    a conceptual table with a well-defined set of columns and unique rows.  It
    is rare for a Relation instance to actually correspond to a concrete
    in-memory table, however; most derived Relation types actually represent an
    operation on some other "base" relation or relations, forming an expression
    tree that can be traversed by visitor classes (see `RelationVisitor`.

    `Relation` is an unusual abstract base class in that the set of derived
    types is closed to the `Leaf` class and the types in the
    `~lsst.daf.relation.operations` subpackage; while external derived classes
    are not explicitly prohibited (there's no graceful way to do that in
    Python), much of the functionality of this package relies on the set of
    derived types enumerated in the `RelationVisitor` interface.  Essentially,
    instead of the types of relations in a tree being extensible, this package
    treats things one can *do* with a relation tree as its primary extension
    interface.

    Relations are associated with "engines": systems that hold the actual data
    a relation (at least) conceptually represents and can perform operations on
    them to obtain the derived data.  These are identified by `EngineTag`
    instances held by relation objects themselves, and the `sql` and
    `iteration` subpackages provide partial implementations of engines for
    relations backed by SQL databases (via `SQLAlchemy`_) and native Python
    iterables, respectively.

    It is up to an engine how strictly its operations adhere to relational
    algebra operation definition.  SQL is formally defined in terms of
    operations on "bags" or "multisets" whose rows are not unique and sometimes
    ordered, while formal relations are always unordered and unique.  The
    `Relation` interface has more a more permissive view of uniqueness to
    facilitate interaction with SQL: `Relation` *may* have non-unique rows, but
    any duplicates are not meaningful, and hence most operations may remove or
    propagate duplicates at their discretion, though engines may make stronger
    guarantees and most relations cannot introduce duplication.  Relation
    instances do track when their rows are guaranteed to be unique, however.
    It is also up to engines to determine how much their operations maintain
    ordering.

    The `Relation` base class provides factory functions that should generally
    be used to construct derived instances (instead of the class constructors).
    These factories perform checking and simplifification that can't be always
    be done in constructors, because they can change the type of the operation
    returned.  In rare cases, the `checked_and_simplified` and
    `assert_checked_and_simplified` methods can be used instead to ensure the
    invariants of derived types are satisfied.

    Relations define `repr` to provide a complete (deserializable via
    `serialization.MappingReader`) string representation, and `str` to provide
    a compact but lossy string representation.

    .. _relational algebra: https://en.wikipedia.org/wiki/Relational_algebra
    .. _SQLAlchemy: https://www.sqlalchemy.org/
    """

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
    ) -> Relation[_T]:
        from .operations import Union

        return Union(engine, columns, (), unique_keys, frozenset(doomed_by)).checked_and_simplified(
            recursive=False
        )

    def __repr__(self) -> str:
        from .serialization import DictWriter

        return json.dumps(self.visit(DictWriter()), indent=2)

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
