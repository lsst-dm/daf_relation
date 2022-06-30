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
    "Engine",
    "JoinConditionState",
    "OrderByTermState",
    "PredicateState",
)

from typing import TYPE_CHECKING, ClassVar, Protocol, final

from lsst.utils.classes import Singleton

from .._columns import _T
from .._engines import EngineOptions
from .._exceptions import EngineError
from ._row_iterable import RowIterable
from ._visitor import IterationVisitor

if TYPE_CHECKING:
    from .._relation import Relation
    from .typing import Row, Sortable


@final
class Engine(metaclass=Singleton):
    """Singleton engine instance that treats relations as iterables of
    mappings.
    """

    __slots__ = ()

    def __hash__(self) -> int:
        return hash(self.__class__)

    def __repr__(self) -> str:
        return "lsst.daf.relation.iteration.engine"

    options: ClassVar[EngineOptions] = EngineOptions(
        flatten_joins=False,
        flatten_unions=True,
        pairwise_joins_only=True,
        pairwise_unions_only=False,
        can_sort=True,
    )

    def execute(self, relation: Relation[_T]) -> RowIterable[_T]:
        """Execute a native iteration relation, returning a Python iterable.

        Parameters
        ----------
        relation : `Relation`
            Relation to execute.

        Returns
        -------
        rows : `RowIterable`
            Iterable over rows, with each row a mapping keyed by `ColumnTag`.
        """
        if relation.engine.tag != self:
            raise EngineError(f"Iteration engine cannot execute relation with engine {relation.engine.tag}.")
        return relation.visit(IterationVisitor())


class PredicateState(Protocol[_T]):
    def __call__(self, row: Row[_T]) -> bool:
        ...


class OrderByTermState(Protocol[_T]):
    def __call__(self, row: Row[_T]) -> Sortable:
        ...


JoinConditionState = PredicateState
