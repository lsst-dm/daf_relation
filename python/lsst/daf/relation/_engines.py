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
from collections.abc import Hashable, Iterator, Set
from typing import ClassVar, Protocol


@dataclasses.dataclass(frozen=True)
class EngineOptions:
    """A struct containing the options that engines can customize to control
    form of the relations they accept.
    """

    flatten_joins: bool = True
    """Whether relation simplification should flatten all adjacent joins in a
    tree into a single `operations.Join` instance (`bool`).
    """

    flatten_unions: bool = True
    """Whether relation simplification should flatten all adjacent unions in a
    tree into a single `operations.Union` instance (`bool`).
    """

    pairwise_joins_only: bool = False
    """Whether relation checks should require that joins have exactly zero or
    two elements (`bool`).

    Zero-element joins are used to represent the unit relation (see
    `Relation.make_unit`), and single-element joins are always simplified away.

    Setting this to `True` requires `flatten_joins` to be `False`.
    """

    pairwise_unions_only: bool = False
    """Whether relation checks should require that unions have exactly zero or
    two elements (`bool`).

    Zero-element unions are used to represent the zero relations (see
    `Relation.make_zero`), and single-element unions are always simplified
    away.

    Setting this to `True` requires `flatten_unions` to be `False`.
    """

    can_sort: bool = True
    """Whether this engine supports sorting (`bool`).
    """

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

    options: ClassVar[EngineOptions]
    """The options that specialize how this engine's relations are checked
    and simplified (`EngineOptions`).
    """


@dataclasses.dataclass(frozen=True)
class EngineTree:
    """A simple tree class that summarizes the engines present in a relation
    tree.

    Iteration over an `EngineTree` is depth-first.
    """

    destination: EngineTag
    """Tag for the final engine htat evaluates this tree.
    (`EngineTag`).
    """

    sources: Set[EngineTree]
    """Set of engine trees whose relations are connected to this one (generally
    indirectly) via transfer operations (`~collections.abc.Set`
    [ `EngineTree` ]).
    """

    @classmethod
    def build_if_needed(cls, destination: EngineTag, sources: Set[EngineTree] = frozenset()) -> EngineTree:
        """Construct a new tree or return an existing one.

        Parameters
        ----------
        destination : `EngineTag`
            Tag to serve as the root of the tree.
        sources : `~collections.abc.Set` [ `EngineTree` ], optional
            Set of source engine trees.  If this has only one element whose
            root is already ``destination``, this element will be returned
            instead of creating a new one.

        Returns
        -------
        tree : `EngineTree`
            Tree with ``tag`` as its root.
        """
        if len(sources) == 1:
            (source,) = sources
            if source.destination == destination:
                return source
        return cls(destination, sources)

    @property
    def depth(self) -> int:
        """The number of levels in the tree (`int`)."""
        return 1 + max((source.depth for source in self.sources), default=0)

    def __contains__(self, tag: EngineTag) -> bool:
        if tag == self.destination:
            return True
        else:
            return any(tag in source for source in self.sources)

    def __iter__(self) -> Iterator[EngineTag]:
        for source in self.sources:
            yield from source
        yield self.destination
