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
from collections.abc import Set
from typing import TYPE_CHECKING, Generic

from lsst.utils.classes import cached_getter

from .._columns import _T
from .._exceptions import ColumnError

if TYPE_CHECKING:
    from ._predicate import Predicate


@dataclasses.dataclass
class JoinCondition(Generic[_T]):

    predicate: Predicate[_T] | None
    lhs_columns: Set[_T]
    rhs_columns: Set[_T]

    @property  # type: ignore
    @cached_getter
    def common_columns(self) -> Set[_T]:
        return self.lhs_columns & self.rhs_columns

    @staticmethod
    def build(predicate: Predicate[_T] | None, all_lhs_columns: Set[_T], all_rhs_columns: Set[_T]) -> JoinCondition[_T]:
        raise NotImplementedError("TODO")

    def __post_init__(self) -> None:
        if self.predicate is not None:
            missing_predicate_columns = set(self.predicate.columns_required)
            missing_predicate_columns.difference_update(self.lhs_columns)
            missing_predicate_columns.difference_update(self.rhs_columns)
            if missing_predicate_columns:
                raise ColumnError(
                    f"Missing predicate columns {missing_predicate_columns} in " "in join condition."
                )
