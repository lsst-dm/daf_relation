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

__all__ = ("CalculationRowIterable",)

from collections.abc import Callable
from typing import TYPE_CHECKING, Iterator

from .._columns import _T
from ._row_iterable import RowIterable

if TYPE_CHECKING:
    from .typing import Row


class CalculationRowIterable(RowIterable[_T]):
    """A `RowIterable` implementation that implements a calculation operation.

    Parameters
    ----------
    base : `RowIterable`
        Original iterable.
    tag : `ColumnTag`
        Key for the new column in result-row mappings.
    callable : `Callable`
        Callable that takes a single mapping argument and returns a new column.
        value.
    """

    def __init__(self, base: RowIterable[_T], tag: _T, callable: Callable[[Row[_T]], bool]):
        self.base = base
        self.tag = tag
        self.callable = callable

    def __iter__(self) -> Iterator[Row[_T]]:
        return ({**row, self.tag: self.callable(row)} for row in self.base)
