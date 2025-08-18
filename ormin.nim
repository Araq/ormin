
type
  DbBackend* {.pure.} = enum
    postgre, sqlite, mysql

import os

template importModel*(backend: DbBackend; filename: string) {.dirty.} =
  ## imports a model from an SQL file.
  bind fileExists, addFileExt, staticExec, ExeExt, parentDir, `/`
  const file = static:
    let path = parentDir(instantiationInfo(-1, true)[0])
    let file = path / filename & ".sql"
    let res = gorgeEx("tools/ormin_importer " & file)
    if res.exitCode != 0:
      raise newException(Exception, "Failed to generate model: " & res.output)
    file
  {.warning: "Imported SQL Model: " & file.}

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
