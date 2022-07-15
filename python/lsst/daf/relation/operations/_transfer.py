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

__all__ = ("Transfer",)

from collections.abc import Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import cached_getter, immutable

from .._columns import _T, UniqueKey
from .._engines import EngineTag
from .._relation import Relation

if TYPE_CHECKING:
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Transfer(Relation[_T]):
    """An operation `.Relation` that indicates a transfer from one engine to
    another.

    Parameters
    ----------
    base : `.Relation`
        Relation this operation acts upon, in the source engine.
    destination : `.EngineTag`
        Engine to transfer the relation to.

    Notes
    -----
    Like other operations, `Transfer` objects should only be constructed
    directly by code that can easily guarantee their `checked_and_simplified`
    invariants; in all other contexts, the `.Relation.transfer` factory should
    be used instead.

    See `.Relation.transfer` for the `checked_and_simplified` behavior for this
    class.
    """

    def __init__(self, base: Relation[_T], destination: EngineTag):
        self.base = base
        self._destination = destination

    base: Relation[_T]
    """Relation this operation acts upon, in the source engine (`.Relation`).
    """

    def __str__(self) -> str:
        return f"{self.base!s} → {self._destination!s}"

    @property  # type: ignore
    @cached_getter
    def engine(self) -> EngineTag:
        # Docstring inherited.
        return self._destination

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self.base.columns

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return self.base.unique_keys

    @property
    def doomed_by(self) -> Set[str]:
        # Docstring inherited.
        return self.base.doomed_by

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_transfer(self)

    def checked_and_simplified(self, recursive: bool = True) -> Relation[_T]:
        # Docstring inherited.
        base = self.base
        if recursive:
            base = base.checked_and_simplified(recursive=True)
        if base.engine == self.engine:
            return base
        match base:
            case Transfer(base=base):
                if base.engine == self.engine:
                    return base
                return Transfer(base, self.engine)
            case _:
                if base is self.base:
                    return self
                return Transfer(base, self.engine)
