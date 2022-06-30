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
    "GeneralIndex",
    "IndexKey",
    "Row",
    "Sortable",
    "UniqueIndex",
)

from typing import Any, Mapping, Protocol, Sequence

from .._columns import _T

Row = Mapping[_T, Any]
"""Generic type alias for a single row."""

IndexKey = tuple[Any, ...]
"""Generic alias for the key type in a mapping that serves as an index."""

UniqueIndex = Mapping[IndexKey, Row[_T]]
"""A mapping that serves as a unique index for a relation."""

GeneralIndex = Mapping[IndexKey, Sequence[Row[_T]]]
"""A mapping that serves as a non-unique index."""


class Sortable(Protocol):
    """A protocol for types that can be sorted."""

    def __lt__(self, other: Any) -> bool:
        ...
