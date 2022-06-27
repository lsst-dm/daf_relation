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

__all__ = ("Leaf",)

from collections.abc import Mapping, Set
from typing import TYPE_CHECKING, Any

from ._columns import _T, UniqueKey, check_unique_keys_in_columns, drop_covered_internal_unique_keys
from ._engines import EngineTag, EngineTree
from ._relation import Relation

if TYPE_CHECKING:
    from ._relation_visitor import _U, RelationVisitor


class Leaf(Relation[_T]):
    """A `Relation` class that represents direct storage of rows, rather than
    an operation on some other relation.

    Most engines are expected to define their own leaf types that inherit from
    this class, but `Leaf` itself is concrete and may be used directly in
    contexts where there is no engine-specific state.

    Parameters
    ----------
    name : `str`
        Name for the relation.  This is used to implement `str` and is part of
        the serialized form of a relation (and hence `repr` as well), but is
        otherwise ignored.
    engine : `EngineTag`
        Identifier for the engine this relation belongs to.
    columns : `~collections.abc.Set`
        Set of columns in the relation.
    unique_keys : `~collections.abc.Set` [ `UniqueKey` ]
        The set of unique constraints this relation is guaranteed to satisfy.
        See `Relation.unique_keys` for details.

    Notes
    -----
    The `Leaf` constructor takes care of simplifying and checking (at least for
    the `columns` and `unique_keys`), allowing `checked_and_simplified` to have
    a trivial implementation that just returns ``self``.  This is possible only
    because `Leaf` instances by definition do not need to recursively check and
    simplify nested relations, and cannot transform their type when simplified.
    """

    def __init__(
        self,
        name: str,
        engine: EngineTag,
        columns: Set[_T],
        unique_keys: Set[UniqueKey[_T]],
    ):
        self._name = name
        self._engine = EngineTree.build_if_needed(engine)
        self._columns = columns
        self._unique_keys = drop_covered_internal_unique_keys(unique_keys)
        check_unique_keys_in_columns(self)

    def __str__(self) -> str:
        return f"{self.name}@{self.engine.tag!s}"

    @property
    def name(self) -> str:
        """Name for the relation (`str`).

        This is used to implement `str` and is part of the serialized form of a
        relation (and hence `repr` as well), but is otherwise ignored.
        """
        return self._name

    @property
    def engine(self) -> EngineTree:
        # Docstring inherited.
        return self._engine

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self._columns

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return self._unique_keys

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_leaf(self)

    def checked_and_simplified(self, *, recursive: bool = True) -> Relation[_T]:
        # Docstring inherited.
        return self

    def write_extra_to_mapping(self) -> Mapping[str, Any]:
        """Transform any extra engine-specific state that should be serialized
        into a mapping with `str` keys and serializable values.

        The default implementation returns an empty dict.

        Returns
        -------
        mapping : `Mapping`
            Possibly-nested mapping containing any extra engine-specific state
            in a form suitable for serialization.  Keys must not include the
            base class constructor's keyword arguments, i.e. "name", "engine",
            "columns", or "unique_keys".
        """
        return {}

    @classmethod
    def from_extra_mapping(
        cls,
        name: str,
        engine: EngineTag,
        columns: Set[_T],
        unique_keys: Set[UniqueKey[_T]],
        extra: Mapping[str, Any],
    ) -> Leaf[_T]:
        """Construct a new `Leaf` instance from a serialization-friendly
        mapping.

        The default implementation calls ``cls`` with all base-class
        constructor arguments positional and ``extra`` unpacked into keyword
        arguments.

        Parameters
        ----------
        name : `str`
            Name for the relation.  This is used to implement `str` and is part
            of the serialized form of a relation (and hence `repr` as well),
            but is otherwise ignored.
        engine : `EngineTag`
            Identifier for the engine this relation belongs to.
        columns : `~collections.abc.Set`
            Set of columns in the relation.
        unique_keys : `~collections.abc.Set` [ `UniqueKey` ]
            The set of unique constraints this relation is guaranteed to
            satisfy.  See `Relation.unique_keys` for details.
        extra : `Mapping`
            Mapping returned by `write_extra_to_mapping`.

        Returns
        -------
        leaf : `Leaf`
            Leaf instance.
        """
        return cls(name, engine, columns, unique_keys, **extra)
