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

__all__ = ("OrderByTerm",)

import dataclasses
from typing import TYPE_CHECKING, Any, Generic

from ._columns import _T

if TYPE_CHECKING:
    from ._engines import EngineTag


@dataclasses.dataclass(frozen=True)
class OrderByTerm(Generic[_T]):
    name: str
    columns_required: frozenset[_T]
    ascending: bool = True
    general_state: dict[str, Any] = dataclasses.field(default_factory=dict)
    engine_state: dict[EngineTag, Any] = dataclasses.field(default_factory=dict, compare=False, repr=False)

    def __str__(self) -> str:
        return self.name

    def reversed(self) -> OrderByTerm[_T]:
        return dataclasses.replace(self, ascending=not self.ascending)
