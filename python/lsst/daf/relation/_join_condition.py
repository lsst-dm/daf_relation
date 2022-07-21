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


import itertools
from abc import abstractmethod
from collections.abc import Iterable, Set
from typing import TYPE_CHECKING, Any, Generic

from ._columns import _T
from ._exceptions import RelationalAlgebraError

if TYPE_CHECKING:
    from ._engines import Engine
    from ._serialization import DictWriter


class JoinCondition(Generic[_T]):
    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def columns_required(self) -> tuple[Set[_T], Set[_T]]:
        """The sets of columns required of the relations this condition joins.
        (`tuple` [ `~collections.abc.Set`, `~collections.abc.Set` ]).
        """

    @abstractmethod
    def supports_engine(self, engine: Engine) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def serialize(self, writer: DictWriter[_T]) -> dict[str, Any]:
        """Transform any state that should be serialized into a mapping with
        `str` keys and serializable values.

        Parameters
        ----------
        writer : `DictWriter`
            Writer object that should be used to save nested serializable
            objects.

        Returns
        -------
        mapping : `dict`
            Possibly-nested `dict` suitable for serialization.
        """
        raise NotImplementedError()

    @abstractmethod
    def flipped(self) -> JoinCondition[_T]:
        raise NotImplementedError()

    @staticmethod
    def find_matching(
        columns0: Set[_T], columns1: Set[_T], /, conditions: Iterable[JoinCondition[_T]]
    ) -> set[JoinCondition[_T]]:
        """Filter a set of `JoinCondition` instances to those that should be
        used to join relations with the given sets of columns.

        Parameters
        ----------
        columns0 : `~collections.abc.Set`
            Columns for one relation to join.
        columns1 : `~collections.abc.Set`
            Columns for the other relation to join.
        conditions : `Iterable` [ `JoinCondition` ]
            Iterable of join conditions to filter.  Both these conditions
            and their flipped copies will be checked against the given columns.

        Returns
        -------
        matched : `set` [ `JoinCondition` ]
            Set of matching join conditions, flipped as needed such that
            ``columns_required[0] <= columns0`` and
            ``columns_required[1] <= columns1``.
        """
        return {
            jc
            for jc in itertools.chain(conditions, (c.flipped() for c in conditions))
            if columns0 >= jc.columns_required[0] and columns1 >= jc.columns_required[1]
        }

    def matches(self, columns0: Set[_T], columns1: Set[_T], /) -> bool:
        return self.columns_required[0] <= columns0 and self.columns_required[1] <= columns1

    def flipped_if_needed(self, columns0: Set[_T], columns1: Set[_T], /) -> JoinCondition[_T]:
        if self.columns_required[0] <= columns0 and self.columns_required[1] <= columns1:
            return self
        elif self.columns_required[1] <= columns0 and self.columns_required[0] <= columns1:
            return self.flipped()
        raise RelationalAlgebraError(
            f"Join condition {self} does not match the columns of the join operands: "
            f"{set(self.columns_required[0]), set(self.columns_required[1])} are not subsets of"
            f"{set(columns0), set(columns1)}."
        )
