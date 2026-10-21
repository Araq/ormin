import ormin/ormin_postgre

proc copyAnOwnedConnection() =
  var original: ManagedDbConn
  var copied = original
  discard original.borrowRaw
  discard copied.borrowRaw

copyAnOwnedConnection()
