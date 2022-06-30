.. py:currentmodule:: lsst.daf.relation

.. _lsst.daf.relation:

#################
lsst.daf.relation
#################

Overview
========

The `Relation` class represents the core concept of `relational algebra`_: a table with a well-defined set of columns and unique rows.
A `Relation` instance does not necessarily correspond to a concrete in-memory table, however; most derived Relation types actually represent an operation on some other "base" relation or relations, forming an expression tree that can be traversed by visitor classes (see `RelationVisitor`).

The core operations on relations are represented by the `Relation` subclasses in the `operations` module, while the `Leaf` class provides an intermediate base class for relations that represent direct storage of rows (and in some cases actually do store rows themselves).
Direct construction of the `operations` classes is not recommended; the `Relation` class itself provides safer factory method that should be used instead.

Engines and operation guarantees
--------------------------------

Relations are associated with "engines": systems that hold the actual data a relation (at least) conceptually represents and can perform operations on them to obtain the derived data.
These are identified by `EngineTag` instances held by relation objects themselves, and the `sql` and `iteration` subpackages provide partial implementations of engines for relations backed by SQL databases (via `SQLAlchemy`_) and native Python iterables, respectively.

It is up to an engine how strictly its operations adhere to relational algebra operation definition.
SQL is formally defined in terms ofoperations on "bags" or "multisets" whose rows are not unique and sometimes ordered, while formal relations are always unordered and unique.
The `Relation` interface has more a more permissive view of uniqueness to facilitate interaction with SQL: `Relation` *may* have non-unique rows, but any duplicates are not meaningful, and hence most operations may remove or propagate duplicates at their discretion, though engines may make stronger guarantees and most relations cannot introduce duplication.
Relation instances do track when their rows are guaranteed to be unique, however.
It is also up to engines to determine how much their operations maintain ordering.

Column Tags
-----------

Relation classes and most other types in this package are generic over a type parameter whose instances satisfy the `ColumnTag` protocol, which is just a  hashable identifier with a `str` representation.
This may be a single final type, a union of types, or a hierarchy.

Relations intentionally do not support column renaming, and instead expect column tags to represent all columns in all tables in an absolute sense: if a column tag in one table is equal to a column tag in some other table, they are expected to mean the same thing in several ways:

- equal values for those columns is automatically included as as constraint when those relations are joined, and which of those columns is "used" as the join's own column with that tag is unspecified and unimportant;
- relations may only be unioned together if they have the same columns;
- `Predicate`, `OrderByTerm`, and `JoinCondition` objects depend only on sets of columns, and do not care which relations actually provide those columns.

It is not required that any particular engine use a `ColumnTag` or its `str` form as its own internal identifier, though this often convenient.
For example, the provided `sql` engine allows `Leaf` relations (which are
usually tables) to have arbitrary column names, but it uses the `ColumnTag` `str` form for names in all SELECT queries that represent operations on those tables.

Hashable builtins such as `str` or `tuple` may be used directly as column tags in simple cases, but it is expected that in most scenarios custom named tuples or frozen dataclasses will be used instead, and some engines or extension operations may require more of the column tag type than is defined here.

.. _relational algebra: https://en.wikipedia.org/wiki/Relational_algebra
.. _SQLAlchemy: https://www.sqlalchemy.org/


.. _lsst.daf.relation-engines:

Provided Engines
================

.. toctree::
   :maxdepth: 1

   iteration.rst

Contributing
============

``lsst.daf.relation`` is developed at https://github.com/lsst-dm/daf_relation.
You can find Jira issues for this module under the `daf_relation <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20daf_relation>`_ component.

.. If there are topics related to developing this module (rather than using it), link to this from a toctree placed here.

.. .. toctree::
..    :maxdepth: 1

.. .. _lsst.daf.relation-pyapi:

Python API reference
====================

.. automodapi:: lsst.daf.relation
   :no-main-docstr:
   :no-inheritance-diagram:
   :include-all-objects:

.. automodapi:: lsst.daf.relation.operations
   :no-inheritance-diagram:
   :include-all-objects:
