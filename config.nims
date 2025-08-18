
task buildimporter, "Build ormin_importer":
  exec "nim c -r tools/ormin_importer"

task test, "Run all test suite":
  buildimporterTask()
  exec "nim c -r tests/tfeature"
  exec "nim c -r tests/tcommon"
  exec "nim c -r tests/tsqlite"
  # Skip PostgreSQL tests as they require a running PostgreSQL server
  # exec "nim c -r -d:postgre tests/tfeature"
  # exec "nim c -r -d:postgre tests/tcommon"
  # exec "nim c -r tests/tpostgre"

task buildexamples, "Build examples: chat and forum":
  buildimporterTask()
  selfExec "c examples/chat/server"
  selfExec "js examples/chat/frontend"
  selfExec "c examples/forum/forum"
  selfExec "c examples/forum/forumproto"
  selfExec "c examples/tweeter/src/tweeter"