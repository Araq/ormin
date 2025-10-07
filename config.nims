switch("nimcache", ".nimcache")

task buildimporter, "Build ormin_importer":
  exec "nim c -o:./tools/ormin_importer tools/ormin_importer"

task clean, "Clean generated files":
  rmFile("tests/forum_model_sqlite.nim")
  rmFile("tests/model_sqlite.nim")

  rmFile("tests/forum_model_postgres.nim")
  rmFile("tests/model_postgre.nim")

task test, "Run all test suite":
  buildimporterTask()
  cleanTask()

  exec "nim c -f -r tests/tfeature"
  exec "nim c -f -r tests/tcommon"
  exec "nim c -f -r tests/tsqlite"
  exec "nim c -f -r tests/tdb_utils"

task setup_postgres, "Ensure local Postgres has test DB/user":
  # Use a simple script to avoid Nim/psql quoting pitfalls
  exec "bash -lc 'bash tools/setup_postgres.sh'"

task test_postgres, "Run PostgreSQL test suite":
  cleanTask()
  buildimporterTask()
  # setup_postgresTask()
  # Pre-generate Postgres models to avoid include timing issues
  exec "./tools/ormin_importer tests/forum_model_postgres.sql"
  exec "./tools/ormin_importer tests/model_postgre.sql"

  exec "nim c -f -d:nimDebugDlOpen -f -d:nimDebugDlOpen -r -d:postgre tests/tfeature"
  exec "nim c -f -d:nimDebugDlOpen -f -d:nimDebugDlOpen -r -d:postgre tests/tcommon"
  exec "nim c -f -d:nimDebugDlOpen -f -d:nimDebugDlOpen -r -d:postgre tests/tpostgre"

task buildexamples, "Build examples: chat and forum":
  buildimporterTask()
  selfExec "c examples/chat/server"
  selfExec "js examples/chat/frontend"
  selfExec "c examples/forum/forum"
  selfExec "c examples/forum/forumproto"
  selfExec "c examples/tweeter/src/tweeter"
