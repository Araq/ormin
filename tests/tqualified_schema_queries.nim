import std/assertions

import ormin

importModel(DbBackend.postgre, "qualified_schema_model", includeStatic = true)

var db {.global.}: DbConn

proc selectPublicEvents() =
  discard query:
    select public.events(public_value)
    where public.events.public_value == "visible"

proc selectAuditEvents() =
  discard query:
    select audit.events(audit_value)

proc selectUsersByUnambiguousBaseName() =
  discard query:
    select users(username)

proc joinQualifiedEvents() =
  discard query:
    select public.events(public_value)
    join audit.events(audit_value) on public.events.public_value == audit.events.audit_value

static:
  doAssert not compiles(block:
    discard query:
      select events(public_value)
  )
