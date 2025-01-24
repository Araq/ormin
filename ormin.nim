
type
  DbBackend* {.pure.} = enum
    postgre, sqlite, mysql

import os

template importModel*(backend: DbBackend; filename: string) {.dirty.} =
  ## imports a model from an SQL file.
  bind fileExists, addFileExt, staticExec, ExeExt, parentDir, `/`
  static:
    #when not fileExists(addFileExt("tools/ormin_importer", ExeExt)):
    #  echo staticExec("nim c tools/ormin_importer", "", "tools/ormin_importer.nim")
    let path = parentDir(instantiationInfo(-1, true)[0])
    echo staticExec("tools/ormin_importer " & (path / filename) & ".sql")

  const dbBackend = backend

  import db_connector/db_common

  when dbBackend == DbBackend.sqlite:
    import ormin/ormin_sqlite
  elif dbBackend == DbBackend.postgre:
    import ormin/ormin_postgre
  elif dbBackend == DbBackend.mysql:
    import ormin/ormin_mysql
  else:
    {.error: "unknown database backend".}

  include filename
  include "ormin/queries"
