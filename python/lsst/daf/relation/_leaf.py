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

from collections.abc import Set
from typing import TYPE_CHECKING, Any, final

from ._columns import _T, UniqueKey, check_unique_keys_in_columns, drop_covered_internal_unique_keys
from ._engines import Engine
from ._relation import Relation

if TYPE_CHECKING:
    from ._relation_visitor import _U, RelationVisitor


@final
class Leaf(Relation[_T]):
    """A `Relation` class that represents direct storage of rows, rather than
    an operation on some other relation.

    Parameters
    ----------
    name : `str`
        Name of the relation.  Used for serialization and string formatting.
    engine : `EngineTag`
        Identifier for the engine this relation belongs to.
    columns : `~collections.abc.Set`
        Set of columns in the relation.
    unique_keys : `~collections.abc.Set` [ `UniqueKey` ]
        The set of unique constraints this relation is guaranteed to satisfy.
        See `Relation.unique_keys` for details.  The base class constructor
        will take care of processing this with
        `drop_covered_internal_unique_keys`.
    parameters : `dict`
        Dictionary of parameters that should be serialized in order to
        reconstruct the relation.
    """

    def __init__(
        self,
        name: str,
        engine: Engine,
        columns: Set[_T],
        unique_keys: Set[UniqueKey[_T]],
        parameters: dict[str, Any],
    ):
        self.name = name
        self.parameters = parameters
        self._engine = engine
        self._columns = columns
        self._unique_keys = drop_covered_internal_unique_keys(unique_keys)
        check_unique_keys_in_columns(self)

    @property
    def engine(self) -> Engine:
        # Docstring inherited.
        return self._engine

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self._columns

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        return self._unique_keys

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_leaf(self)
