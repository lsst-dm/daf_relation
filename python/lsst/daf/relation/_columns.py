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

__all__ = (
    "ColumnTag",
    "UniqueKey",
    "check_unique_keys_in_columns",
    "is_unique_key_covered",
    "drop_covered_internal_unique_keys",
)

import itertools
from typing import TYPE_CHECKING, AbstractSet, Hashable, Protocol, TypeVar

from ._exceptions import ColumnError

_T = TypeVar("_T", bound="ColumnTag")

if TYPE_CHECKING:
    from ._relation import Relation


class ColumnTag(Hashable, Protocol):
    def __str__(self) -> str:
        ...


UniqueKey = frozenset
"""Type alias for an immutable set of columns representing a unique constraint
(`type`).
"""


def check_unique_keys_in_columns(relation: Relation[_T]) -> None:
    """Check that all unique key sets in a relation include only columns that
    are actually in the relation.

    This is a utility function for implementations of
    `Relation.checked_and_simplified`; it should not be necessary for other
    code to call it directly.

    Parameters
    ----------
    relation : `Relation`
        Relation to check.

    Raises
    ------
    ColumnError
        Raised if one or more columns in a unique key is not in the relation.
    """
    for k in relation.unique_keys:
        if not k.issubset(relation.columns):
            raise ColumnError(
                f"Unique key {k} for relation {relation!r} involves columns "
                f"{set(k - relation.columns)} not in the relation."
            )


def is_unique_key_covered(key: UniqueKey, base_keys: AbstractSet[UniqueKey]):
    return key in base_keys or any(key.issuperset(base_key) for base_key in base_keys)


def drop_covered_internal_unique_keys(keys: AbstractSet[UniqueKey]) -> set[UniqueKey]:
    keys = set(keys)
    while True:
        to_drop = {k1 for k1, k2 in itertools.permutations(keys) if k1.issuperset(k2)}
        if to_drop:
            keys.difference_update(to_drop)
        else:
            return keys
