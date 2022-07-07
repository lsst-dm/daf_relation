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

__all__ = ("Predicate",)

import dataclasses
from typing import TYPE_CHECKING, Any, Generic

from ._columns import _T

if TYPE_CHECKING:
    from ._engines import EngineTag


@dataclasses.dataclass(frozen=True)
class Predicate(Generic[_T]):
    """A helper struct for relation trees that represents a boolean-returning
    unary function in a selection.

    Notes
    -----
    Like other relation helpers (`JoinCondition`, `OrderByTerm`), a single
    `Predicate` instance can support multiple engines, and store custom state
    for each of these.
    """

    name: str
    """Name of the predicate (`str`).

    This is used as the `str` representation, and is included in both equality
    comparison and serialization.
    """

    columns_required: frozenset[_T]
    """The columns required to compute this expression (`frozenset`).
    """

    general_state: dict[str, Any] = dataclasses.field(default_factory=dict, compare=True, hash=False)
    """State for the predicate that is independent of any engine (`dict`).

    This state is included in equality comparison and serialization. To support
    serialization via nested dictionary formats like JSON or YAML, this must
    (recursively) contain only types supported by that format.

    While this is a mutable mapping, the expectation is that its contents will
    not change after the `Predicate` is added to a `Relation`; this is
    important because these objects are frequently held by sets that rely on
    equality comparisons not changing.
    """

    engine_state: dict[EngineTag, Any] = dataclasses.field(default_factory=dict, compare=False, repr=False)
    """State for the join condition that is engine-dependent.

    This state is not included in equality comparison or serialization.
    Instead, concrete implementations of `serialization.MappingReader` are
    expected to reconstruct any needed per-engine state from `name`,
    `columns_required`, and `general_state`.

    Values are frequently callables with engine-specific signatures.
    """

    def __str__(self) -> str:
        return self.name
