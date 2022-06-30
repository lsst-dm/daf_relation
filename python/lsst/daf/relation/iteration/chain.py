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

__all__ = ("ChainRowIterable",)

import itertools
from typing import TYPE_CHECKING, Iterator, Sequence

from .._columns import _T
from ._row_iterable import RowIterable

if TYPE_CHECKING:
    from .typing import Row


class ChainRowIterable(RowIterable[_T]):
    """A `RowIterable` implementation that wraps `itertools.chain`.

    Parameters
    ----------
    chain : `Sequence` [ `RowIterable` ]
        Sequence of iterables to chain together.
    """

    def __init__(self, chain: Sequence[RowIterable[_T]]):
        self.chain = chain

    def __iter__(self) -> Iterator[Row[_T]]:
        return itertools.chain.from_iterable(self.chain)
