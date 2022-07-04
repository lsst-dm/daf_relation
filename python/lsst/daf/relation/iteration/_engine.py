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
    "ExtensionInterface",
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
    """Singleton engine class that for relations as iterables of mappings."""

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
        relation : `.Relation`
            Relation to execute.

        Returns
        -------
        rows : `RowIterable`
            Iterable over rows, with each row a mapping keyed by `.ColumnTag`.
        """
        if relation.engine.tag != self:
            raise EngineError(f"Iteration engine cannot execute relation with engine {relation.engine.tag}.")
        return relation.visit(IterationVisitor())


class ExtensionInterface(Protocol[_T]):
    """Interface for `.Extension` relations in this engine.

    `.Extension` subclasses must implement this interface to be used in the
    native iteration engine.
    """

    def to_row_iterable(self) -> RowIterable[_T]:
        """Evaluate the extension operation.

        Parameters
        ----------
        extension : `.Extension`
            Extension relation the callable was attached to.

        Returns
        -------
        rows : `RowIterable`
            The result of the operation.
        """
        ...


class PredicateState(Protocol[_T]):
    """Callable protocol for the values of `.Predicate.engine_state` for this
    engine.

    This is also used as the type for `.JoinCondition` state, as join
    conditions are applied as predicates after performing natural joins on any
    common columns.  A join condition's state for this engine may also be
    `None` if and only if the second `RowIterable` in the join implements
    `RowIterable.try_join` to handle it.
    """

    def __call__(self, row: Row[_T]) -> bool:
        """Evaluate the predicate.

        Parameters
        ----------
        row : `Mapping`
            Mapping from `.ColumnTag` to actual column values, representing a
            row in the relation.

        Returns
        -------
        keep : `bool`
            Whether to include this row in the new relation.
        """
        ...


class OrderByTermState(Protocol[_T]):
    """Callable protocol for the values of `.OrderByTerm.engine_state` for this
    engine.
    """

    def __call__(self, row: Row[_T]) -> Sortable:
        """Evaluate the order-by term.

        Parameters
        ----------
        row : `Mapping`
            Mapping from `.ColumnTag` to actual column values, representing a
            row in the relation.

        Returns
        -------
        sortable
            Arbitrary Python object that implements at least less-than
            comparison.
        """
        ...


JoinConditionState = PredicateState
