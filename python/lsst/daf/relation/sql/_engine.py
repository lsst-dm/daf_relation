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

__all__ = ("Engine",)

import dataclasses
from typing import TYPE_CHECKING, ClassVar, Sequence, final

import sqlalchemy

from .._columns import _T
from .._engines import CheckAndSimplifyOptions
from .._exceptions import EngineError
from .to_executable import ToExecutable
from ._select_parts import SelectParts

if TYPE_CHECKING:
    from .._relation import Relation
    from .._order_by_term import OrderByTerm
    from ._column_type_info import ColumnTypeInfo, _L


@final
@dataclasses.dataclass(frozen=True, slots=True)
class Engine:

    database: str

    def __repr__(self) -> str:
        return f"lsst.daf.relation.sql.Engine({self.database!r})"

    options: ClassVar[CheckAndSimplifyOptions] = CheckAndSimplifyOptions(
        flatten_joins=True,
        flatten_unions=True,
        pairwise_joins_only=False,
        pairwise_unions_only=False,
        sliced_sorts_only=False,
        sorted_slices_only=False,
    )

    def to_executable(
        self,
        relation: Relation[_T],
        column_types: ColumnTypeInfo[_T, _L],
        *,
        distinct: bool = False,
        order_by: Sequence[OrderByTerm[_T]] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> sqlalchemy.sql.expression.SelectBase:
        if relation.engine.tag != self:
            raise EngineError(f"Iteration engine cannot execute relation with engine {relation.engine.tag}.")
        return relation.visit(ToExecutable(column_types, distinct, order_by, offset, limit))

    def to_from_clause(
        self, relation: Relation[_T], column_types: ColumnTypeInfo[_T, _L]
    ) -> sqlalchemy.sql.FromClause:
        if relation.engine.tag != self:
            raise EngineError(f"Iteration engine cannot execute relation with engine {relation.engine.tag}.")
        select_parts = SelectParts.from_relation(relation, column_types)
        if not select_parts.where and select_parts.columns_available is None:
            return select_parts.from_clause
        return select_parts.to_executable(relation, column_types).subquery()
