## Ormin -- ORM for Nim.
## (c) 2017 Andreas Rumpf
## MIT License.

import parsesql, streams, strutils, os, parseopt, tables, db_common

#import compiler / [ast, renderer]

const
  FileHeader = """
type
  Attr = object
    name: string
    tabIndex: int
    typ: DbTypekind
    key: int   # 0 nothing special,
               # +1 -- primary key
               # -N -- references attribute N
"""

proc writeHelp() =
  echo """
ormin <schema.sql> --out:<file.nim>  --db:postgre|sqlite|mysql
"""

proc writeVersion() = echo "v1.0"

type
  DbColumn* = object   ## information about a database column
    name*: string      ## name of the column
    tableName*: string ## name of the table the column belongs to (optional)
    typ*: DbType       ## type of the column
    primaryKey*: bool  ## is this a primary key?
    refs*: (string, string)  ## is this a foreign key?
  DbColumns* = seq[DbColumn]

  KnownTables = OrderedTable[string, DbColumns]
  Target = enum
    postgre, sqlite, mysql

proc hasAttribute(colDesc: SqlNode; k: set[SqlNodeKind]): bool =
  for i in 2..<colDesc.len:
    if colDesc[i].kind in k: return true

proc hasRefs(colDesc: SqlNode): (string, string) =
  for i in 2..<colDesc.len:
    let c = colDesc[i]
    if c.kind == nkReferences:
      assert c[0].kind == nkCall
      return (c[0][0].strVal, c[0][1].strVal)
  return ("", "")

proc getType(n: SqlNode): DbType =
  var it = n
  if it.kind == nkCall: it = it[0]
  if it.kind == nkEnumDef:
    result.kind = dbEnum
    result.validValues = @[]
    for i in 0..<it.len:
      assert it[i].kind == nkStringLit
      result.validValues.add it[i].strVal
  elif it.kind in {nkIdent, nkStringLit}:
    var k = dbUnknown
    case it.strVal.toLowerAscii
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
    of "timestamp": k = dbTimestamp
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
    result.kind = k
    result.name = it.strVal

proc collectTables(n: SqlNode; t: var KnownTables) =
  case n.kind
  of nkCreateTable, nkCreateTableIfNotExists:
    let tableName = n[0].strVal
    var cols: DbColumns = @[]
    for i in 1..<n.len:
      let it = n[i]
      if it.kind == nkColumnDef:
        var typ = getType(it[1])
        if hasAttribute(it, {nkNotNull}):
          typ.notNull = true
        cols.add DbColumn(name: it[0].strVal, tableName: tableName,
                          typ: typ,
                          primaryKey: hasAttribute(it, {nkPrimaryKey}),
                          refs: hasRefs(it))
    for i in 1..<n.len:
      let it = n[i]
      if it.kind == nkForeignKey:
        var r = it[1]
        doAssert r.kind == nkReferences
        r = r[0]
        doAssert r.kind == nkCall
        for c in mitems(cols):
          if cmpIgnoreCase(c.name, it[0].strVal) == 0:
            c.refs = (r[0].strVal, r[1].strVal)
            break
    t[tablename] = cols
  else:
    for i in 0..<n.len: collectTables(n[i], t)

proc attrToKey(a: DbColumn; t: KnownTables): int =
  if a.primaryKey: return 1
  if a.refs[0].len > 0:
    var i = 0
    for k, v in pairs(t):
      for b in v:
        if cmpIgnoreCase(k, a.refs[0]) == 0 and cmpIgnoreCase(b.name, a.refs[1]) == 0:
          return -i - 1
        inc i
  result = 0

proc generateCode(infile, outfile: string; target: Target) =
  let sql = parseSql(newFileStream(infile, fmRead), infile)
  var knownTables = initOrderedTable[string, DbColumns]()
  collectTables(sql, knownTables)
  var f: File
  if open(f, outfile, fmWrite):
    f.write FileHeader
    f.write "const tableNames = ["
    var i = 0
    for k in keys(knownTables):
      if i > 0: f.write ",\L  "
      else: f.write "\L  "
      f.write(escape(k))
      inc i
    f.write "\L]\L"
    i = 0
    var j = 0
    f.write "\Lconst attributes = ["
    for k, v in mpairs(knownTables):
      for a in v:
        if j > 0: f.write ",\L  "
        else: f.write "\L  "
        f.write "Attr(name: ", escape(a.name), ", tabIndex: ", $i,
                ", typ: ", $a.typ.kind, ", key: ", $attrToKey(a, knownTables), ")"
        inc j
      inc i
    f.write "\L]\L"
    close(f)

var p = initOptParser()
var infile = ""
var outfile = ""
var target: Target
for kind, key, val in p.getopt():
  case kind
  of cmdArgument:
    infile = key
  of cmdLongOption, cmdShortOption:
    case key
    of "help", "h": writeHelp()
    of "version", "v": writeVersion()
    of "out", "o": outfile = val
    of "db": target = parseEnum[Target](val)
    else: discard
  of cmdEnd: assert(false) # cannot happen
if infile == "":
  # no filename has been given, so we show the help:
  writeHelp()
else:
  if outfile == "":
    outfile = changeFileExt(infile, "nim")
  generateCode(infile, outfile, target)