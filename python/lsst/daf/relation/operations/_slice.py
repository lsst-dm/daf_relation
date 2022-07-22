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

__all__ = ("Slice",)

from collections.abc import Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import immutable

from .._columns import _T, UniqueKey
from .._exceptions import ColumnError, EngineError
from .._relation import Relation

if TYPE_CHECKING:
    from .. import column_expressions
    from .._engine import Engine
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Slice(Relation[_T]):
    """An operation `.Relation` that sorts and/or filters rows positionally.

    Parameters
    ----------
    base : `.Relation`
        Relation this operation acts upon.
    order_by : `tuple` [ `.OrderByTerm`, ... ]
        Criteria for sorting rows.
    offset : `int`
        Starting index (from 0) for the rows included.
    limit : `int` or `None`
        Maximum number of rows returned, or `None` for no limit.

    Notes
    -----
    Like other operations, `Slice` objects should only be constructed directly
    by code that can easily guarantee their `checked_and_simplified`
    invariants; in all other contexts, the `.Relation.slice` factory should be
    used instead.

    See `.Relation.slice` for the `checked_and_simplified` behavior for this
    class.
    """

    def __init__(
        self,
        base: Relation,
        order_by: tuple[column_expressions.OrderByTerm[_T], ...],
        offset: int,
        limit: int | None,
    ):
        for o in order_by:
            if base.engine is not None and not o.expression.is_supported_by(base.engine):
                raise EngineError(f"Order-by term {o} does not support engine {base.engine}.")
            if not o.expression.columns_required <= base.columns:
                raise ColumnError(
                    f"Order-by term {o} for base relation {self.base} needs "
                    f"columns {o.expression.columns_required - self.base.columns}."
                )
        self.base = base
        self.order_by = order_by
        self.offset = offset
        self.limit = limit

    base: Relation[_T]
    """Relation this operation acts upon (`.Relation`).
    """

    order_by: tuple[column_expressions.OrderByTerm[_T], ...]
    """Criteria for sorting rows (`tuple` [ `.OrderByTerm`, ... ])."""

    offset: int
    """Starting index (from 0) for the rows included (`int`)."""

    limit: int | None
    """Maximum number of rows returned, or `None` for no limit (`int` or `
    None`)."""

    def __str__(self) -> str:
        return (
            f"slice({self.base!s}, order_by={', '.join(str(o) for o in self.order_by)}], "
            f"offset={self.offset}, limit={self.limit})"
        )

    @property
    def engine(self) -> Engine[_T]:
        # Docstring inherited.
        return self.base.engine

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self.base.columns

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return self.base.unique_keys

    def _try_selection(self, predicate: column_expressions.Predicate[_T]) -> Relation[_T] | None:
        # Docstring inherited.
        if (result := super()._try_selection(predicate)) is not None:
            return result
        # Selections and slices only commute when the slice is just a sort.
        if not self.offset and self.limit is None:
            if (new_base := self.base._try_selection(predicate)) is not None:
                return Slice(new_base, self.order_by, offset=0, limit=None)
        return None

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_slice(self)
