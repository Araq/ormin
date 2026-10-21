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

  # These suites share test.db and must execute serially.
  exec("atlas-run tests --jobs:1 tests/tfeature.nim tests/tcommon.nim " &
    "tests/tsqlite.nim tests/tdb_utils.nim tests/timportstatic.nim " &
    "tests/ttransactions.nim tests/tpostgres_schema_import.nim " &
    "tests/tqualified_schema_queries.nim")
  exec "atlas-run tests --jobs:2 tests/tquery_types.nim -- -d:release"

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

  # These suites share one PostgreSQL database and must execute serially.
  exec("atlas-run tests --jobs:1 tests/tfeature.nim tests/tcommon.nim " &
    "tests/tpostgre.nim tests/tpostgre_statement_lifecycle.nim -- " &
    "-d:nimDebugDlOpen -d:postgre")
  exec("atlas-run tests --jobs:2 tests/tquery_types.nim -- " &
    "-d:nimDebugDlOpen -d:release -d:postgre")

task buildexamples, "Build examples: chat and forum":
  buildimporterTask()
  selfExec "c examples/chat/server"
  selfExec "js examples/chat/frontend"
  selfExec "c examples/forum/forum"
  selfExec "c examples/forum/forumproto"
  selfExec "c examples/tweeter/src/tweeter"
