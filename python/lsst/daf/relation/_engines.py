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

__all__ = ("EngineTag", "EngineTree")

import dataclasses
from typing import Callable, Hashable, Mapping, Protocol, TypeVar


class EngineTag(Hashable, Protocol):
    pass


_V = TypeVar("_V")


@dataclasses.dataclass(frozen=True)
class EngineTree:
    tag: EngineTag
    sources: Mapping[EngineTag, EngineTree]

    def find(
        self, tag: EngineTag, initial: _V, recurse: Callable[[EngineTree], _V | None]
    ) -> _V | None:
        if self.tag == tag:
            # Found it, start unrolling the recursion.
            return initial
        else:
            # Recurse until we find the given tag.
            for source_tree in self.sources.values():
                if (result := recurse(source_tree)) is not None:
                    # Found our tag downstream, keep unrolling the
                    # recursion.
                    return result
            # This branch is a dead-end.
            return None
