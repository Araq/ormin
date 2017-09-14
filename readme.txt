================
     ormin
================

Prepared SQL statement generator for Nim. A lightweight ORM.

Features:

- Compile time query checking: Types as well as table
  and column names are checked, no surprises at runtime!
- Automatic join generation: Ormin knows the table
  relations and can compute the "natural" join for you!
- Nim based DSL for queries: No syntax errors at runtime,
  no SQL injections possible.
- Generated prepared statements: As fast as low level
  hand written API calls!

Todo:

- Add support for UNION, INTERSECT and EXCEPT.
- Add support 'ORDER BY ASC/DESC'.
- Add support for LIMIT and OFFSET.
- First class JSON support: No explicit conversions
  from rows to JSON objects required.
- Write mysql backend.


Copyright (c) 2017 Andreas Rumpf.
All rights reserved.
