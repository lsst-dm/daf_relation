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
from typing import AbstractSet, Hashable, Iterator, Protocol


class EngineTag(Hashable, Protocol):
    pass


@dataclasses.dataclass(frozen=True)
class EngineTree:
    tag: EngineTag
    sources: AbstractSet[EngineTree]

    @classmethod
    def build(cls, tag: EngineTag, sources: AbstractSet[EngineTree] = frozenset()) -> EngineTree:
        if len(sources) == 1:
            (source,) = sources
            if source.tag == tag:
                return source
        return cls(tag, sources)

    def backtrack_from(self, tag: EngineTag) -> Iterator[EngineTag]:
        if tag == self.tag:
            yield tag
        else:
            for source in self.sources:
                yield from source.backtrack_from(tag)

    @property
    def depth(self) -> int:
        return 1 + max(source.depth for source in self.sources)
