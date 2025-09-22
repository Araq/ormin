import std/strutils
import db_connector/db_common

proc dbTypFromName*(name: string): DbTypeKind =
  var k = dbUnknown

  case name.toLowerAscii
  of "int", "integer", "int8", "smallint", "int16",
      "longint", "int32", "int64", "tinyint", "hugeint": k = dbInt
  of "uint", "uint8", "uint16", "uint32", "uint64": k = dbUInt
  of "serial": k = dbSerial
  of "bit": k = dbBit
  of "bool", "boolean": k = dbBool
  of "blob": k = dbBlob
  of "fixedchar": k = dbFixedChar
  of "varchar", "text", "string": k = dbVarchar
  of "json": k = dbJson
  of "xml": k = dbXml
  of "decimal": k = dbDecimal
  of "float", "double", "longdouble", "real": k = dbFloat
  of "date", "day": k = dbDate
  of "time": k = dbTime
  of "datetime": k = dbDateTime
  of "timestamp", "timestamptz": k = dbTimestamp
  of "timeinterval": k = dbTimeInterval
  of "set": k = dbSet
  of "array": k = dbArray
  of "composite": k = dbComposite
  of "url", "uri": k = dbUrl
  of "uuid": k = dbUuid
  of "inet", "ip", "tcpip": k = dbInet
  of "mac", "macaddress": k = dbMacAddress
  of "geometry": k = dbGeometry
  of "point": k = dbPoint
  of "line": k = dbLine
  of "lseg": k = dbLseg
  of "box": k = dbBox
  of "path": k = dbPath
  of "polygon": k = dbPolygon
  of "circle": k = dbCircle
  else: discard

  return k
