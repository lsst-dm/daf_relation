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

__all__ = ("JoinCondition",)


import dataclasses
import itertools
from typing import TYPE_CHECKING, AbstractSet, Any, Generic, Iterable

from ._columns import _T

if TYPE_CHECKING:
    from ._engines import EngineTag


@dataclasses.dataclass(frozen=True, eq=False)
class JoinCondition(Generic[_T]):
    name: str
    columns_required: tuple[frozenset[_T], frozenset[_T]]
    general_state: dict[str, Any] = dataclasses.field(default_factory=dict)
    engine_state: dict[EngineTag, Any] = dataclasses.field(default_factory=dict, repr=False)
    was_flipped: bool = dataclasses.field(default=False, repr=False)

    def __eq__(self, other: Any) -> bool:
        if self.__class__ == other.__class__:
            return self.name == other.name and (
                frozenset(self.columns_required) == frozenset(other.columns_required)
                and self.general_state == other.general_state
            )
        else:
            return NotImplemented

    def __hash__(self) -> int:
        # general_state is part of equality comparison, but is expected to
        # rarely play a role there (as it'll usually be empty).  Including it
        # in the hash would be a pain (dict is not hashable) and not obviously
        # helpful, since unequal things are allowed to have the same hash.
        return hash((self.name, frozenset(self.columns_required)))

    def flipped(self) -> JoinCondition[_T]:
        return dataclasses.replace(
            self, columns_required=self.columns_required[::-1], was_flipped=not self.was_flipped
        )

    @staticmethod
    def find_matching(
        columns0: AbstractSet[_T], columns1: AbstractSet[_T], /, conditions: Iterable[JoinCondition[_T]]
    ) -> set[JoinCondition[_T]]:
        return {
            jc
            for jc in itertools.chain(conditions, (c.flipped() for c in conditions))
            if columns0 >= jc.columns_required[0] and columns1 >= jc.columns_required[1]
        }
