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

task test_postgres, "Run PostgreSQL test suite":
  # Skip PostgreSQL tests as they require a running PostgreSQL server
  cleanTask()

  exec "nim c -r -d:postgre tests/tfeature"
  exec "nim c -r -d:postgre tests/tcommon"
  exec "nim c -r tests/tpostgre"

task buildexamples, "Build examples: chat and forum":
  buildimporterTask()
  selfExec "c examples/chat/server"
  selfExec "js examples/chat/frontend"
  selfExec "c examples/forum/forum"
  selfExec "c examples/forum/forumproto"
  selfExec "c examples/tweeter/src/tweeter"