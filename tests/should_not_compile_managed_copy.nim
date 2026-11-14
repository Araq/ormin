when defined(sqlite):
  import ormin/ormin_sqlite
else:
  import ormin/ormin_postgre

when defined(copyManagedStatement):
  proc copyAnOwnedStatement() =
    var original: ManagedStmt
    var copied = original
    finalizeStmt(original)
    finalizeStmt(copied)
  copyAnOwnedStatement()
else:
  proc copyAnOwnedConnection() =
    var original: ManagedDbConn
    var copied = original
    discard original.borrowRaw
    discard copied.borrowRaw
  copyAnOwnedConnection()
