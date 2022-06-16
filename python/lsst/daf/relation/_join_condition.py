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
from typing import TYPE_CHECKING, Any, Generic, Iterable

if TYPE_CHECKING:
    from ._column_tag import _T
    from ._engine_tag import EngineTag
    from ._relation import Relation


@dataclasses.dataclass(frozen=True)
class JoinCondition(Generic[_T]):
    name: str
    columns_required: tuple[frozenset[_T], frozenset[_T]]
    state: dict[EngineTag, Any] = dataclasses.field(default_factory=dict, compare=False, repr=False)

    def match(self, relations: Iterable[Relation[_T]]) -> list[tuple[int, int]]:
        c0, c1 = self.columns_required
        result: list[tuple[int, int]] = []
        for (i0, r0), (i1, r1) in itertools.permutations(enumerate(relations), 2):
            if c0 <= r0.columns and c1 <= r1.columns:
                result.append((i0, i1))
        return result
