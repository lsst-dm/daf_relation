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

__all__ = ("EngineTag", "EngineTree", "EngineOptions")

import dataclasses
from typing import AbstractSet, Hashable, Iterator, Protocol


@dataclasses.dataclass(frozen=True)
class EngineOptions:
    flatten_joins: bool = True
    flatten_unions: bool = True
    pairwise_joins_only: bool = False
    pairwise_unions_only: bool = False
    sliced_sorts_only: bool = False
    sorted_slices_only: bool = False

    def __post_init__(self) -> None:
        if self.pairwise_joins_only and self.flatten_joins:
            raise ValueError(
                "Inconsistent options: cannot require pairwise joins when joins are being flattened."
            )
        if self.pairwise_unions_only and self.flatten_unions:
            raise ValueError(
                "Inconsistent options: cannot require pairwise unions when unions are being flattened."
            )


class EngineTag(Hashable, Protocol):
    def __str__(self) -> str:
        ...

    @property
    def options(self) -> EngineOptions:
        ...


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

    def iter_from(self, tag: EngineTag) -> Iterator[EngineTag]:
        if tag == self.tag:
            yield tag
        else:
            for source in self.sources:
                yield from source.iter_from(tag)

    @property
    def depth(self) -> int:
        return 1 + max((source.depth for source in self.sources), default=0)

    def __contains__(self, tag: EngineTag) -> bool:
        if tag == self.tag:
            return True
        else:
            return any(tag in source for source in self.sources)

    def __iter__(self) -> Iterator[EngineTag]:
        for source in self.sources:
            yield from source
        yield self.tag
