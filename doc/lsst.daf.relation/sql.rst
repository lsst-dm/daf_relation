.. py:currentmodule:: lsst.daf.relation.sql

.. _lsst.daf.relation-sql:

SQLAlchemy query generation (`lsst.daf.relation.sql`)
-----------------------------------------------------

Overview
""""""""

The engine provided by the `lsst.daf.relation.sql` package translates `.Relation` trees to `SQLAlchemy`_ expressions.
It doesn't actually execute those expressions, however; running a SQL query and fetching its results back to a Python client is really best considered a `~.operations.Transfer` between engines, with a SQL engine like this one as the source and an engine backed by in-memory Python objects (such as `lsst.daf.relation.iteration`) as its destination.

This engine flattens joins and unions, and it actually reorders any combination of adjacent `joins <.Relation.join>`, `selections <.Relation.selection>`, and `projections <.Relation.projection>` into a single ``SELECT...FROM...WHERE`` statement.
It assumes the database query optimizer will reorder at least these operations itself anyway, so the goal is to keep the query as simple as possible to stay out of its way and aid human headers.

The `.EngineTag` class for this engine is a simple dataclass, `lsst.daf.relation.sql.Engine`, with a single `~lsst.daf.relation.sql.Engine.name` string attribute that is used only for comparisons and string formatting; this may be a connection URI, a schema name, or essentially anything else.
The `Engine.to_executable` method is the main entry point for the engine.
All of the code in this package other than `Engine` itself can also be used
a custom `.EngineTag` type.

`ColumnTypeInfo` and Logical Columns
""""""""""""""""""""""""""""""""""""

All operations on the SQL engine require an instance of the `ColumnTypeInfo` class.
`ColumnTypeInfo` is generic with two type parameters: the `.ColumnTag` parameter it shares
with `.Relation` and most other `.lsst.daf.relation` classes, and another we call the "logical column type".
In the simplest case, the logical column type is just `sqlalchemy.sql.ColumnElement`, and this is what the default implementations of most `ColumnTypeInfo` methods assume, but custom subclasses can use other types, such as wrappers that hold one or more `sqlalchemy.sql.ColumnElement` objects.
This allows one column tag and logical column in the `.Relation` expression of query to map to multiple columns in the SQL representation.

Support for `.Extension` operation subclasses can be added by either reimplementing `ColumnTypeInfo.to_executable` and `ColumnTypeInfo.to_select_parts`, or by including the `ExtensionInterface` methods in the `.Extension` subclass.

Leaf types
""""""""""

The SQL engine uses a single leaf relation type, `SelectPartsLeaf`, with a `SelectParts` struct as an attribute.
These both have the same generic type parameterization as `ColumnTypeInfo`.

The interpretation of `.JoinCondition`, `.OrderByTerm`, and `.Predicate` objects is delegated to `ColumnTypeInfo` and can be completely customized, but by default these are expected to have per-engine state set to callables with signatures defined by `JoinConditionState`, `OrderByTermState`, and `PredicateState`, respectively.
For `.OrderByTerm` only, the per-engine state may also be `None` if `~OrderByTerm.columns_requested` has unit length, which indicates that the associated logical column should used directly.

.. _SQLAlchemy: https://www.sqlalchemy.org/

API reference
"""""""""""""

.. automodapi:: lsst.daf.relation.sql
   :no-heading:
   :no-inheritance-diagram:
   :include-all-objects:
