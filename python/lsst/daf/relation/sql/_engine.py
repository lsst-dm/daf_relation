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
from collections.abc import Sequence
from typing import TYPE_CHECKING, ClassVar, final

import sqlalchemy

from .._columns import _T
from .._engines import EngineOptions
from .._exceptions import EngineError
from ._to_executable import ToExecutable

if TYPE_CHECKING:
    from .._order_by_term import OrderByTerm
    from .._relation import Relation
    from ._column_type_info import _L, ColumnTypeInfo


@final
@dataclasses.dataclass(frozen=True, slots=True)
class Engine:
    """Engine tag class for converting relation trees to SQLAlchemy
    executables.
    """

    name: str
    """Name that identifies this engine relative to others of the same type.
    """

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"lsst.daf.relation.sql.Engine({self.name!r})"

    options: ClassVar[EngineOptions] = EngineOptions(
        flatten_joins=True,
        flatten_unions=True,
        pairwise_joins_only=False,
        pairwise_unions_only=False,
        can_sort=True,
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
        """Convert a relation tree to an executable SQLAlchemy expression.

        Parameters
        ----------
        relation : `.Relation`
            Root of the relation tree to convert.
        column_types : `ColumnTypeInfo`
            Object that relates column tags to logical columns.
        distinct : `bool`
            Whether to generate an expression whose rows are forced to be
            unique.
        order_by : `Iterable` [ `.OrderByTerm` ]
            Iterable of objects that specify a sort order.
        offset : `int`, optional
            Starting index for returned rows, with ``0`` as the first row.
        limit : `int` or `None`, optional
            Maximum number of rows returned, or `None` (default) for no limit.

        Returns
        -------
        select : `sqlalchemy.sql.expression.SelectBase`
            A SQLAlchemy SELECT or compound SELECT query.

        Raises
        ------
        EngineError
            Raised if the relation's engine is not the same as ``self``, or if
            the tree contains any transfers.
        """
        if relation.engine.tag != self:
            raise EngineError(f"Iteration engine cannot execute relation with engine {relation.engine.tag}.")
        return relation.visit(ToExecutable(column_types, distinct, order_by, offset, limit))
