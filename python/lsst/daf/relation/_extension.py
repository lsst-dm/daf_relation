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

__all__ = ("Extension",)

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from ._columns import _T
from ._engines import EngineTag, EngineTree
from ._exceptions import EngineError
from ._relation import Relation

if TYPE_CHECKING:
    from ._relation_visitor import _U, RelationVisitor
    from ._serialization import DictWriter


class Extension(Relation[_T]):
    """A `Relation` intermediate abstract base class for custom operations."""

    @property
    @abstractmethod
    def base(self) -> Relation[_T]:
        """Base relation this operation acts on (`Relation`)."""
        raise NotImplementedError()

    @property
    def engine(self) -> EngineTree:
        # Docstring inherited.
        return self.base.engine

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_extension(self)

    def checked_and_simplified(self, *, recursive: bool = True) -> Relation[_T]:
        # Docstring inherited.
        if recursive:
            base = self.base.checked_and_simplified(recursive=True)
        else:
            base = self.base
        if not self.supports_engine(base.engine.tag):
            raise EngineError(f"Extension operation {self} does not support engine {base.engine.tag}.")
        if base is not self.base:
            return self.rebased(base, equivalent=True)
        return self

    @abstractmethod
    def supports_engine(self, engine: EngineTag) -> bool:
        """Test whether this extension supports the given engine.

        Parameters
        ----------
        engine : `EngineTag`
            Engine to test.

        Returns
        -------
        supported : `bool`
            Whether the given engine is supported by this extension.
        """
        raise NotImplementedError()

    @abstractmethod
    def rebased(self, base: Relation[_T], *, equivalent: bool) -> Relation[_T]:
        """Create a copy of this operation with a new base relation.

        Parameters
        ----------
        base : `Relation`
            New base relation for the operation.
        equivalent : `bool`
            If `True`, the new base relation is guaranteed to have the same
            rows, columns, unique keys, and engine tree as the original.

        Returns
        -------
        rebased : `Relation`
            A new version of this operation, with all simplifications and
            checks done.  Due to simplification this may not be an instance of
            the original extension type anymore.
        """
        raise NotImplementedError()

    @abstractmethod
    def serialize(self, writer: DictWriter[_T]) -> dict[str, Any]:
        raise NotImplementedError()
