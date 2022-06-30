.. py:currentmodule:: lsst.daf.relation.iteration

.. _lsst.daf.relation-iteration:

Native iteration (`lsst.daf.relation.iteration`)
------------------------------------------------

Overview
""""""""

The engine tag for iteration is the singleton `Engine` class, which is a singleton available as `lsst.daf.relation.iteration.engine`.
Its `~Engine.execute` method is the main entry point for evaluating trees of relations that are purely in this engine.

This is a simple engine intended primarily to serve as a "final transfer destination" for relation trees that are mostly defined in other engines (e.g. `~lsst.daf.relation.sql`), as a way to enable iteration over rows in Python and limited Python-side postprocessing.
That can include:

- applying predicates defined as regular Python callables;
- performing simple joins to bring in locally-cached values;
- concatenating, sorting, and deduplicating results in memory.

The iteration engine requires `pairwise joins <EngineOptions.pairwise_joins_only>`, as it doesn't have a solver for otherwise determining the best order in which to join multiple relations.
But it also guarantees that joins will preserve row order, with the first relation's ordering taking precedence over the second for common columns.
All other operations preserve row order as well.

It also attempts to use lazy evaluation by performing operations row-by-row via generators, but this is not always possible.
In particular:

- `~Relation.distinct` operations build a `dict` containing all rows.
- `~Relation.join` operations need a similar `dict` containing all rows in the second operand in the join (unless specialized; see `RowIterable.try_join`), and will build one if needed.
- `~Relation.slice` operations build a `list` and sort it (and the full list is sorted before any slicing is performed).

Execution of a relation tree takes care to only ever iterate once, *unless* the iteration target is already an in-memory collection.  In other words, we always
first build an in-memory collection from general iterables (which are assumed to be slower or higher-overhead) whenever we need to iterate over the rows more than once.

Leaf types
""""""""""

The iteration engine has two leaf relation types: `RowIterableLeaf` and its subclass `SerializableRowIterableLeaf`.
These differ only in whether the rows are included when written to a nested
dict for serialization.
Both need to be constructed with a `RowIterable`, which may be a custom subclass of that ABC or a concrete `RowCollection` instance backed by an in-memory collection.

`JoinCondition` or `Predicate` per-engine state typically a callable that accepts a mapping containing at least all of the required columns (keyed by `ColumnTag`) and returns `bool`, indicating whether the row should be preserved.
The `PredicateState` and `JoinCondition` protocols provide a formal definition for these callables.

Similarly, `OrderByTerm` per-engine state is typically a callable that accepts the same kind of mapping and returns a sortable object (note that `operator.itemgetter` can thus be used to create a trivial order-by term that sorts by a single column).
The ascending/descending flip is handled outside the callable.

Extension points
""""""""""""""""

Both `RowIterable` and `RowCollection` can be subclassed further to customize the behavior of some relation operations, and the `visitor.InspectionVisitor` class can be subclassed (or fully rewritten) and used in place of `Engine.execute` for full customization.

The `~RowIterable.try_selection`, `~RowIterable.try_join`, and
`~RowIterable.try_slice` may also be overridden by subclasses to provide
special handling of predicates, join conditions, or order-by terms,
respectively, without per-engine state.
But note that adding these customizations on a leaf relation's `RowIterable` will have no effect if the desired operation doesn't act directly on the leaf operation, since any intermediate operation will create a new `RowIterable` instance of some kind with the default implementations of those methods.

API reference
"""""""""""""

.. automodapi:: lsst.daf.relation.iteration
   :no-heading:
   :no-inheritance-diagram:
   :include-all-objects:
