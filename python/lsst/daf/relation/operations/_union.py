# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the relations of the GNU General Public License as published by
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

__all__ = ("Union",)

from collections.abc import Set
from typing import TYPE_CHECKING, final

from lsst.utils.classes import immutable

from .._columns import _T, UniqueKey, check_unique_keys_in_columns, is_unique_key_covered
from .._engines import EngineTag
from .._exceptions import ColumnError, EngineError
from .._relation import Relation, Null

if TYPE_CHECKING:
    from .._join_condition import JoinCondition
    from .._predicate import Predicate
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Union(Relation[_T]):
    """An operation `.Relation` that combines the rows of its input relations.

    Parameters
    ----------
    engine : `.EngineTag`
        Engine the union is performed in.  This must be the same as the engine
        of all input relations.
    columns : `~collections.abc.Set` [ `.ColumnTag` ]
        Set of columns for this relation; all input relations must have the
        same columns already.
    relations : `tuple` [ `.Relation` , ... ]
        Input relations to combine.
    unique_keys : `~collections.abc.Set` [ `.UniqueKey` ]
        Set of sets that represent multi-column unique constraints that will be
        *naturally* satisfied by this union, even if the engine does not take
        any extra action to remove duplicates.  If not provided or empty, the
        returned relation does not guarantee uniqueness.

    Notes
    -----
    Like other operations, `Union` objects should only be constructed directly
    by code that can easily guarantee their `checked_and_simplified`
    invariants; in all other contexts, the `.Relation.union` factory should be
    used instead.

    Union objects with no relations are permitted (with no conditions, either),
    and are used to represent the "unit relation" (see `.Relation.make_zero`),
    though these are often simplified out after they are added to larger
    relation trees.  Union objects with one relation are not permitted; these
    should always be simplified out as a no-op.  Some relations may prohibit
    unions with more than two relations (see
    `EngineOptions.pairwise_unions_only`).

    This union operation corresponds SQL's ``UNION ALL``, because it does not
    force unique rows in the result (at most it can be told when unique rows
    naturally occur).  Use ``union(...).distinct(...)`` for an operation that
    maps to SQL's ``UNION``.

    See `.Relation.union` for the `checked_and_simplified` behavior for this
    class.
    """

    def __init__(
        self,
        engine: EngineTag,
        columns: Set[_T],
        relations: tuple[Relation[_T], ...] = (),
        unique_keys: Set[UniqueKey[_T]] = UniqueKey(),
    ):
        self._engine = engine
        self._columns = columns
        self.relations = relations
        self._unique_keys = unique_keys

    def __str__(self) -> str:
        return f"({'∪ '.join(str(r) for r in self.relations)})"

    relations: tuple[Relation[_T], ...]
    """Input relations for the union (`tuple` [ `.Relation`, ... ])."""

    extra_doomed_by: frozenset[str]
    """Diagnostic messages that can be used to report why the relation
    has no rows, when that is the case (`~collections.abc.Set` [ `str` ]).

    This is included in `doomed_by` only when there are no input relations or
    all input relations also have no rows.
    """

    @property
    def engine(self) -> EngineTag:
        # Docstring inherited.
        return self._engine

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self._columns

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return self._unique_keys

    def visit(self, visitor: RelationVisitor[_T, _U]) -> _U:
        # Docstring inherited.
        return visitor.visit_union(self)

    def checked_and_simplified(self, *, recursive: bool = True) -> Relation[_T]:
        # Docstring inherited.
        relations_flat: list[Relation[_T]] = []
        extra_doomed_by_flat: set[str] = set()
        any_changes = False
        for original in self.relations:
            if recursive:
                relation = original.checked_and_simplified(recursive=True)
                any_changes = any_changes or relation is not original
            else:
                relation = original
            if self.engine.options.flatten_unions:
                match relation:
                    case Union(relations=relations, extra_doomed_by=extra_doomed_by):
                        relations_flat.extend(relations)
                        extra_doomed_by_flat.update(extra_doomed_by)
                        any_changes = True
                    case _:
                        relations_flat.append(relation)
            else:
                relations_flat.append(relation)

        if len(relations_flat) == 0:
            return Null(self.engine, self.columns)
        if len(relations_flat) == 1 and not extra_doomed_by_flat:
            return relations_flat[0]
        if self.engine.options.pairwise_unions_only:
            if len(relations_flat) > 2:
                raise EngineError(f"Engine {self.engine} requires pairwise unions only.")
        check_unique_keys_in_columns(self)
        for relation in relations_flat:
            for key in self.unique_keys:
                if not is_unique_key_covered(key, relation.unique_keys):
                    raise ColumnError(
                        f"Union is declared to have unique key {set(key)}, but "
                        f"member {relation} is not unique with those columns."
                    )
            if relation.engine != self.engine:
                raise EngineError(
                    f"Union member {relation} has engine {relation.engine}, "
                    f"while union has {self.engine}."
                )
            if relation.columns != self.columns:
                raise ColumnError(
                    f"Mismatched union columns: {set(relation.columns)} != {set(self.columns)} "
                    f"for relation {relation}."
                )
        if not any_changes:
            return self
        else:
            return Union(
                self.engine,
                self.columns,
                tuple(relations_flat),
                frozenset(),
            )

    def try_insert_join(self, other: Relation[_T], conditions: Set[JoinCondition[_T]]) -> Relation[_T] | None:
        # Docstring inherited.
        new_relations: list[Relation[_T]] = []
        for nested_relation in self.relations:
            if (new_relation := nested_relation.try_insert_join(other, conditions)) is not None:
                new_relations.append(new_relation)
            else:
                return None
        return Union(self.engine, self.columns, tuple(new_relations), self.unique_keys)

    def try_insert_selection(self, predicate: Predicate[_T]) -> Relation[_T] | None:
        # Docstring inherited.
        new_relations: list[Relation[_T]] = []
        for nested_relation in self.relations:
            if (new_relation := nested_relation.try_insert_selection(predicate)) is not None:
                new_relations.append(new_relation)
            else:
                return None
        return Union(
            self.engine,
            self._columns,
            tuple(new_relations),
            unique_keys=self.unique_keys,
        ).assert_checked_and_simplified()
