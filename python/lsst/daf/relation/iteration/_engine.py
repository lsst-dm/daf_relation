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
    "JoinConditionInterface",
    "OrderByTermInterface",
    "PredicateInterface",
)

from typing import TYPE_CHECKING, Protocol

from .._columns import _T
from .._engines import Engine as BaseEngine
from .._exceptions import EngineError
from ._row_iterable import RowIterable

if TYPE_CHECKING:
    from .._leaf import Leaf
    from .._relation import Relation
    from .typing import Row, Sortable


class Engine(BaseEngine):
    def __init__(self, name: str) -> None:
        self.name = name
        self.leaf_cache: dict[Leaf, RowIterable] = {}

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"lsst.daf.relation.iteration.Engine({self.name!r})"

    def evaluate_leaf(self, leaf: Leaf[_T]) -> RowIterable[_T]:
        return self.leaf_cache[leaf]

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
        from ._visitor import IterationVisitor

        if relation.engine is not self:
            raise EngineError(
                f"Engine {self!r} cannot operate on relation {relation} with engine {relation.engine!r}."
            )
        return relation.visit(IterationVisitor(self))


class PredicateInterface(Protocol[_T]):
    def test_iteration_row(self, row: Row[_T]) -> bool:
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


class OrderByTermInterface(Protocol[_T]):
    def get_iteration_row_sort_key(self, row: Row[_T]) -> Sortable:
        """Evaluate the order-by term.

        Parameters
        ----------
        row : `Mapping`
            Mapping from `.ColumnTag` to actual column values, representing a
            row in the relation.

        Returns
        -------
        key
            Arbitrary Python object that implements at least less-than
            comparison.
        """
        ...

    def get_iteration_row_sort_reverse(self) -> bool:
        ...


JoinConditionInterface = PredicateInterface
