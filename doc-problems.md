Known issues in docs
====================

Build warnings
--------------

For example,
`doc/lsst.daf.relation/index.rst:65: WARNING: autosummary: stub file not found 'lsst.daf.relation.UniqueKey'. Check your autosummary_generate setting.`

I strongly suspect this is <https://github.com/sphinx-doc/sphinx/issues/8963>

Missing docs for instance attributes documented via class attributes
--------------------------------------------------------------------

For example, most attributes of `JoinCondition` (but not `was_flipped`).

Another (non-dataclass) example is `operations.Distinct.base`.

`:include-all-objects:` is set, so that's not it.

It seems the only ones that get docs are those actually assigned values other
than `dataclasses.field`, which would be a very unfortunate limitation, even if
it's understandable why it might happen (properties also get attribute docs,
but of course, not all attributes are properties).

Attributes summary section has more info than the full attributes docs
----------------------------------------------------------------------

For example, see `operations.Distinct`; my theory is that it's inherited
attributes (maybe across module boundaries?) that are only getting their docs
populated in the summary section, with the full docs just having the attribute
name.
