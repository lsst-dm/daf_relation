# This file is part of daf_butler.
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
from typing import TYPE_CHECKING, AbstractSet, Any, Generic

if TYPE_CHECKING:
    from ._column_tag import _T


@dataclasses.dataclass(frozen=True, eq=False)
class JoinCondition(Generic[_T]):
    state: dict[str, Any]
    columns_required: tuple[AbstractSet[_T], AbstractSet[_T]]
    reverse: bool = False

    def reversed(self) -> JoinCondition[_T]:
        return dataclasses.replace(
            self, columns_required=tuple(reversed(self.columns_required)), reverse=not self.reverse
        )
