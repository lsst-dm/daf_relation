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

from lsst.utils.classes import cached_getter, immutable

from .._columns import _T, UniqueKey, check_unique_keys_in_columns, is_unique_key_covered
from .._engines import EngineTag, EngineTree
from .._exceptions import ColumnError, EngineError
from .._relation import Relation

if TYPE_CHECKING:
    from .._relation_visitor import _U, RelationVisitor


@final
@immutable
class Union(Relation[_T]):
    """An operation `.Relation` that combines the rows of its input relations.

    Parameters
    ----------
    engine : `.EngineTag`
        Engine the join is performed in.  This must be the same as the engine
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
    extra_doomed_by : `frozenset` [ `str` ]
        Diagnostic messages that can be used to report why the relation has no
        rows when that is the case.  This should generally be provided when
        there are no relations.  When there are other relations, it is only
        used as (part of) the `doomed_by` property when those relations also
        yield no rows.

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
        extra_doomed_by: frozenset[str] = frozenset(),
    ):
        self._engine = engine
        self._columns = columns
        self.relations = relations
        self._unique_keys = unique_keys
        self.extra_doomed_by = extra_doomed_by

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

    @property  # type: ignore
    @cached_getter
    def engines(self) -> EngineTree:
        # Docstring inherited.
        return EngineTree.build_if_needed(self._engine, {r.engines for r in self.relations})

    @property
    def columns(self) -> Set[_T]:
        # Docstring inherited.
        return self._columns

    @property
    def unique_keys(self) -> Set[UniqueKey[_T]]:
        # Docstring inherited.
        return self._unique_keys

    @property  # type: ignore
    @cached_getter
    def doomed_by(self) -> Set[str]:
        # Docstring inherited.
        result = set(self.extra_doomed_by)
        for related in self.relations:
            if not related.doomed_by:
                return frozenset()
            result.update(related.doomed_by)
        return result

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
            if self.engines.destination.options.flatten_unions:
                match relation:
                    case Union(relations=relations, extra_doomed_by=extra_doomed_by):
                        relations_flat.extend(relations)
                        extra_doomed_by_flat.update(extra_doomed_by)
                        any_changes = True
                    case _:
                        relations_flat.append(relation)
            else:
                relations_flat.append(relation)

        if len(relations_flat) == 1 and not extra_doomed_by_flat:
            return relations_flat[0]
        if self.engines.destination.options.pairwise_unions_only:
            if len(relations_flat) > 2:
                raise EngineError(f"Engine {self.engines.destination} requires pairwise unions only.")
        check_unique_keys_in_columns(self)
        for relation in relations_flat:
            for key in self.unique_keys:
                if not is_unique_key_covered(key, relation.unique_keys):
                    raise ColumnError(
                        f"Union is declared to have unique key {set(key)}, but "
                        f"member {relation} is not unique with those columns."
                    )
            if relation.engines != self.engines:
                raise EngineError(
                    f"Union member {relation} has engine {relation.engines}, "
                    f"while union has {self.engines}."
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
                self.engines.destination,
                self.columns,
                tuple(relations_flat),
                self.unique_keys,
                frozenset(extra_doomed_by_flat),
            )
