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
    "drop_covered_internal_unique_keys",
    "is_unique_key_covered",
)

import itertools
from collections.abc import Hashable, Set
from typing import TYPE_CHECKING, Protocol, TypeVar

from ._exceptions import ColumnError

_T = TypeVar("_T", bound="ColumnTag")

if TYPE_CHECKING:
    from ._relation import Relation


class ColumnTag(Hashable, Protocol):
    """An interface for objects that represent columns in a relation.

    Notes
    -----
    Relations intentionally do not support column renaming, and instead expect
    to be used along with a `ColumnTag` class (or set of classes) whose
    instances can represent all columns in all tables in an absolute sense: if
    a column in one table is equal to a column in some other table, they are
    expected to mean the same thing in several ways:

    - equal values for that columns is automatically included as as constraint
      when those relations are joined, and which of those columns is "used" as
      the join's own column with that tag is unspecified and unimportant;
    - relations may only be unioned together if they have the same columns;
    - `Predicate`, `OrderByTerm`, and `JoinCondition` objects depend only on
      sets of columns, and do not care which relations actually provide those
      columns.

    It is not required that any particular engine use a `ColumnTag` or its
    `str` form as its own internal name, though this often convenient.  For
    example, the provided `sql` engine allows `Leaf` relations (which are
    usually tables) to have arbitrary column names, but it uses the `ColumnTag`
    `str` form for names in all SELECT queries that represent operations on
    those tables.

    Hashable builtins such as `str` or `tuple` may be used directly as column
    tags in simple cases, but it is expected that in most scenarios custom
    named tuples or frozen dataclasses will be used instead, and some engines
    or extension operations may require more of the column tag type than is
    defined here.
    """

    def __str__(self) -> str:
        ...


# This would ideally be a `typing.NewType`, not just an alias, for better type
# safety, but those aren't allowed to be generic
# (https://github.com/python/mypy/issues/3331).
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
        Raised if a column in the relation's unique keys is not in the
        relation's columns.
    """
    for k in relation.unique_keys:
        if not k.issubset(relation.columns):
            raise ColumnError(
                f"Unique key {k} for relation {relation!r} involves columns "
                f"{set(k - relation.columns)} not in the relation."
            )


def is_unique_key_covered(key: UniqueKey, base_keys: Set[UniqueKey]) -> bool:
    """Test whether a unique key (immutable set of columns) is a superset of
    any of the given unique keys.

    A unique key ``a`` is *covered* by another unique key ``b`` when ``a ⊃ b``
    because any relation that has unique rows when projected to ``b`` must also
    have unique rows when projected to ``a``.

    Parameters
    ----------
    key : `UniqueKey`
        Set of columns representing a unique constraint.
    base_keys : `UniqueKey`
        Set of sets of columns that will be checked to see if any element is
        a subset of ``key``.

    Returns
    -------
    covered : `bool`
        Whether ``key`` is a superset of any element of ``base_keys``.
    """
    return key in base_keys or any(key.issuperset(base_key) for base_key in base_keys)


def drop_covered_internal_unique_keys(keys: Set[UniqueKey]) -> set[UniqueKey]:
    """Return a set of unique keys in which no key is covered by any other.

    Parameters
    ----------
    keys : `~collections.abc.Set` [ `UniqueKey` ]
        Starting set of unique keys.

    Returns
    -------
    new_keys : `set` [ `UniqueKey` ]
        Modified copy of ``keys`` with any keys that are covered by any other
        key removed.

    See Also
    --------
    is_unique_key_covered
    """
    keys = set(keys)
    while True:
        to_drop = {k1 for k1, k2 in itertools.permutations(keys) if k1.issuperset(k2)}
        if to_drop:
            keys.difference_update(to_drop)
        else:
            return keys
