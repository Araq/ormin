
type
  DbBackend* {.pure.} = enum
    postgre, sqlite, mysql

import os
from ormin/importer_core import generateModelCode, ImportTarget

template importModel*(backend: DbBackend, filename: string, includeStatic: static[bool] = false) {.dirty.} =
  ## imports a model from an SQL file.
  bind fileExists, parentDir, `/`
  bind generateModelCode, ImportTarget
  const file = static:
    let path = parentDir(instantiationInfo(-1, true)[0])
    path / filename & ".sql"
  const importedFile = static:
    if not includeStatic:
      let res =
        if fileExists("./tools/ormin_importer"):
          gorgeEx("./tools/ormin_importer " & file)
        else:
          # run ormin_importer from the PATH
          gorgeEx("ormin_importer " & file)
      if res.exitCode != 0:
        raise newException(Exception, "Failed to generate model: " & res.output)
    file
  {.warning: "Imported SQL Model: " & importedFile.}

  const dbBackend = backend

  import db_connector/db_common
  when includeStatic:
    import ormin/db_utils

  when dbBackend == DbBackend.sqlite:
    import ormin/ormin_sqlite
  elif dbBackend == DbBackend.postgre:
    import ormin/ormin_postgre
  elif dbBackend == DbBackend.mysql:
    import ormin/ormin_mysql
  else:
    {.error: "unknown database backend".}

  when includeStatic:
    generateModelCode(staticRead(file), file, ImportTarget(ord(backend)), true)
  else:
    include filename
  include "ormin/queries"
