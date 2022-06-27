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
from collections.abc import Iterable, Set
from typing import TYPE_CHECKING, Any, Generic

from ._columns import _T

if TYPE_CHECKING:
    from ._engines import EngineTag


@dataclasses.dataclass(frozen=True, eq=False)
class JoinCondition(Generic[_T]):
    """A helper struct for relation trees that represents a non-equality
    constraint in a join.

    Notes
    -----
    Like other relation helpers (`Predicate`, `OrderByTerm`), a single
    `JoinCondition` instance can support multiple engines, and store custom
    state for each of these.

    `JoinCondition` holds the columns required of each of the two relations it
    joins in a 2-tuple of sets, with a boolean `was_flipped` attribute to track
    when those are swapped to match the order in which the relations are joined
    together.  Despite this, a pair of `JoinCondition` objects that differ only
    in whether they are flipped will compare (and hash) as equal, because it
    is useful to be able to put `JoinCondition` objects in a set under this
    definition of equality.
    """

    name: str
    """Name of the join condition (`str`).

    This is used as the `str` representation, and is included in both equality
    comparison and serialization.
    """

    columns_required: tuple[frozenset[_T], frozenset[_T]]
    """The sets of columns required of the relations this condition joins.
    (`tuple` [ `frozenset`, `frozenset` ]).

    This is included equality comparison and serialization, but the order of
    the tuple is not included in equality comparison.
    """

    general_state: dict[str, Any] = dataclasses.field(default_factory=dict)
    """State for the join condition that is independent of any engine (`dict`).

    This state is included in equality comparison and serialization.  It is not
    modified when the conditions is flipped.  To support serialization via
    nested dictionary formats like JSON or YAML, this must (recursively)
    contain only types supported by that format.

    While this is a mutable mapping, the expectation is that its contents will
    not change after the `JoinCondition` is added to a `Relation`; this is
    important because these objects are frequently held by sets that rely on
    equality comparisons not changing.
    """

    engine_state: dict[EngineTag, Any] = dataclasses.field(default_factory=dict, repr=False)
    """State for the join condition that is engine-dependent.

    This state is not included in equality comparison or serialization.
    Instead, concrete implementations of `serialization.MappingReader` are
    expected to reconstruct any needed per-engine state from `name`,
    `columns_required`, and `general_state`.

    Values are frequently callables with engine-specific signatures.
    """

    was_flipped: bool = dataclasses.field(default=False, repr=False)
    """Whether this join condition was flipped (its tuple of column sets
    reordered) after construction (`bool`).

    Because `general_state` and `engine_state` are not modified when a join
    condition is flipped, engines should usually pass `was_flipped` to any
    callable stored within them.

    As a rule, `Relation` objects should only hold `JoinCondition` instances
    with `was_flipped` `False`, because joins aren't flipped until engines
    actually operate on the tree, and this should always occur on local
    variables and other temporaries.  As a result, `Relation` serialization
    does not include this flag.
    """

    def __str__(self) -> str:
        return self.name

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
        """Return a flipped copy of this condition.

        Returns
        -------
        flipped : `JoinCondition`
            Condition with `columns_required` elements swapped and
            `was_flipped` inverted.
        """
        return dataclasses.replace(
            self, columns_required=self.columns_required[::-1], was_flipped=not self.was_flipped
        )

    @staticmethod
    def find_matching(
        columns0: Set[_T], columns1: Set[_T], /, conditions: Iterable[JoinCondition[_T]]
    ) -> set[JoinCondition[_T]]:
        """Filter a set of `JoinCondition` instances to those that should be
        used to join relations with the given sets of columns.

        Parameters
        ----------
        columns0 : `~collections.abc.Set`
            Columns for one relation to join.
        columns1 : `~collections.abc.Set`
            Columns for the other relation to join.
        conditions : `Iterable` [ `JoinCondition` ]
            Iterable of join conditions to filter.  Both these conditions
            and their flipped copies will be checked against the given columns.

        Returns
        -------
        matched : `set` [ `JoinCondition` ]
            Set of matching join conditions, flipped as needed such that
            ``columns_required[0] <= columns0`` and
            ``columns_required[1] <= columns1``.
        """
        return {
            jc
            for jc in itertools.chain(conditions, (c.flipped() for c in conditions))
            if columns0 >= jc.columns_required[0] and columns1 >= jc.columns_required[1]
        }
