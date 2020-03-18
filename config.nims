import strformat, strutils
from os import `/`

let
  testDir = thisDir() / "tests"
  commonTest = testDir / "tforum"
  commonFuncTest = testDir / "tfunction"

proc parseArgs(): string =
  let numParams = paramCount()
  # not include the first and last param
  for i in 1..<numParams:
    if i != 1: result.add " "
    result.add paramStr(i)

task run, "Run":
  --run
  setCommand "c"

# Init database tasks
task initcommonsqlite, "Init sqlite for common test":
  let
     dbFile = testDir / "test.db"
     sqlFile = testDir / "forum_model_sqlite.sql"
  if existsFile(dbFile): exec &"rm {dbFile}"
  exec &"sqlite3 -init {sqlFile} tests/test.db << EOF .quit EOF"

task initcommonpostgre, "Init postgresql for common test":
  let
    sqlFile = testDir / "forum_model_postgres.sql"
    sql = staticRead(sqlFile)
  var tables = newSeq[string]()
  for line in sql.splitLines():
    if line.strip().startsWith("create table"): 
      let
        s = line.rfind(' ')
        table = line[s..^1].strip(chars={'('})
      tables.add(table)
  exec &"""psql -U test -wq -h localhost -c "drop table if exists {tables.join(",")} cascade"
  """
  exec &"psql -U test -wq -h localhost -f {sqlFile}"

proc initCommonDb(args: string) = 
  if args.contains("-d:postgre"):
    selfExec "initcommonpostgre"
  else:
    selfExec "initcommonsqlite"

# Test common feature tasks:
task tcommon, "Test common features":
  let args = parseArgs()
  initCommonDb(args)
  selfExec &"run {args} {commonTest}"

task tfunction, "Test common sql functions":
  let args = parseArgs()
  initCommonDb(args)
  selfExec &"run {args} {commonFuncTest}"