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

__all__ = ("Distinct",)

from collections.abc import Sequence, Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import immutable

from .._columns import _T, UniqueKey, check_unique_keys_in_columns, drop_covered_internal_unique_keys
from .._engines import EngineTag
from .._exceptions import RelationalAlgebraError
from .._relation import Relation

if TYPE_CHECKING:
    from .._relation_visitor import _U, RelationVisitor
    from .._join_condition import JoinCondition
    from .._predicate import Predicate


@final
@immutable
class Distinct(Relation[_T]):
    """An operation `.Relation` that guarantees unique rows.

    Parameters
    ----------
    base : `.Relation`
        Relation this operation acts upon.
    unique_keys : `~collections.abc.Set` [ `.UniqueKey` ]
        Sets of columns that are sufficient to guarantee unique rows;
        projecting this relation to a superset of any of these sets of
        columns will preserving uniqueness.

    Notes
    -----
    Like other operations, `Distinct` objects should only be constructed
    directly by code that can easily guarantee their `checked_and_simplified`
    invariants; in all other contexts, the `.Relation.distinct` factory should
    be used instead.

    See `.Relation.distinct` for the `checked_and_simplified` behavior for this
    class.
    """

    def __init__(self, base: Relation[_T], unique_keys: Set[UniqueKey[_T]]):
        self.base = base
        self._unique_keys = unique_keys

    base: Relation[_T]
    """Relation this operation acts upon (`.Relation`).
    """

    def __str__(self) -> str:
        return f"distinct {self.base!s}"

    @property
    def engine(self) -> EngineTag:
        # Docstring inherited.
        return self.base.engine

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self.base.columns

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return self._unique_keys

    @property
    def doomed_by(self) -> Set[str]:
        # Docstring inherited.
        return self.base.doomed_by

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_distinct(self)

    def checked_and_simplified(self, recursive: bool = True) -> Relation[_T]:
        # Docstring inherited.
        base = self.base
        if recursive:
            base = base.checked_and_simplified(recursive=True)
        if not self.unique_keys:
            raise RelationalAlgebraError(f"Distinct operation on {base} does not define any unique keys.")
        unique_keys = drop_covered_internal_unique_keys(self.unique_keys)
        if base.unique_keys:
            if base.unique_keys == self.unique_keys:
                return base
            else:
                raise RelationalAlgebraError(
                    f"Base relation {base} is already unique on keys {base.unique_keys}, "
                    f"but distinct operation declares it to be unique on {self.unique_keys}."
                )
        check_unique_keys_in_columns(self)
        if unique_keys == self.unique_keys and base is self.base:
            return self
        else:
            return Distinct(base, unique_keys)

    def try_insert_join(self, other: Relation[_T], conditions: Set[JoinCondition[_T]]) -> Relation[_T] | None:
        # Docstring inherited.
        return None

    def try_insert_selection(self, predicate: Predicate[_T]) -> Relation[_T] | None:
        # Docstring inherited.
        if (new_base := self.base.try_insert_selection(predicate)) is not None:
            return Distinct(new_base, self.unique_keys).assert_checked_and_simplified(recursive=False)
        return None
