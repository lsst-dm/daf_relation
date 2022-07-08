# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("SelectionRowIterable",)

from typing import TYPE_CHECKING, Iterator

from .._columns import _T
from ._row_iterable import RowIterable

if TYPE_CHECKING:
    from ._engine import PredicateInterface
    from .typing import Row


class SelectionRowIterable(RowIterable[_T]):
    """A `RowIterable` implementation that implements a selection operation.

    Parameters
    ----------
    base : `RowIterable`
        Original iterable to filter rows from.
    predicates : `tuple` [ `PredicateState` ]
        Tuple of callables with the `PredicateState` signature.
    """

    def __init__(self, base: RowIterable[_T], predicates: tuple[PredicateInterface[_T], ...]):
        self.base = base
        self.predicates = predicates

    def __iter__(self) -> Iterator[Row[_T]]:
        return (row for row in self.base if all(p.test_iteration_row(row) for p in self.predicates))
