
type
  DbBackend* {.pure.} = enum
    postgre, sqlite, mysql

import os

template importModel*(backend: DbBackend; path, filename: string) {.dirty.} =
  ## imports a model from an SQL file.
  bind fileExists, addFileExt, staticExec, ExeExt, `/`
  static:
    when not fileExists(addFileExt("tools/ormin_importer", ExeExt)):
      echo staticExec("nim c tools/ormin_importer", "", "tools/ormin_importer.nim")
    echo staticExec("tools/ormin_importer " & (path / filename) & ".sql")

  const dbBackend = backend

  import db_common
  include filename
  include "ormin/queries"
