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

__all__ = (
    "IndexKey",
    "JoinConditionState",
    "OrderByTermState",
    "PredicateState",
    "Row",
    "SequenceIndex",
    "Sortable",
    "UniqueIndex",
)

from typing import Any, Mapping, Protocol, Sequence

from .._columns import _T

Row = Mapping[_T, Any]
IndexKey = tuple[Any, ...]
UniqueIndex = Mapping[IndexKey, Row[_T]]
SequenceIndex = Mapping[IndexKey, Sequence[Row[_T]]]


class Sortable(Protocol):
    def __lt__(self, other: Any) -> bool:
        ...


class PredicateState(Protocol[_T]):
    def __call__(self, row: Row[_T]) -> bool:
        ...


class OrderByTermState(Protocol[_T]):
    def __call__(self, row: Row[_T]) -> Sortable:
        ...


JoinConditionState = PredicateState
