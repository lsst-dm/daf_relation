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

__all__ = ("Engine",)

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._columns import _T
    from ._leaf import Leaf


class Engine(ABC):
    """An interface for objects that serve as identifiers for engines.

    Notes
    -----
    The "engines" used to evaluate relation trees do not have a common
    interface, because they all do different things.  This class defines what
    they must have in common: a hashable, equality-comparable class (preferably
    lightweight with a concise `str` representation) used to identify the
    engine in relations and the various helper objects that are also part of a
    relation tree (`Predicate`, `JoinCondition`, `OrderByTerm`).

    It is recommended that an engine's tag class also serve as the primary
    entry point for its most important operations.
    """

    def __str__(self) -> str:
        ...

    @abstractmethod
    def evaluate_leaf(self, leaf: Leaf[_T]) -> Any:
        ...

    @abstractmethod
    def get_column_function(self, name: str) -> Any | None:
        ...
