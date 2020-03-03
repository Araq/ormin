
import ormin, ormin/serverws, json

importModel(DbBackend.sqlite, "forum_model")

static:
  dbTypeMap.add(dbInet, "string")
  
var db {.global.} = open("stuff", "", "", "")

protocol "forumclient.nim":
  common:
    typemap:
      kstring = string
    when defined(js):
      type kstring = cstring
    else:
      type kstring = string

  server:
    query:
      delete antibot
      where ip == %arg
  client:
    proc deleteAntibot(ip: string)
  server:
    query:
      update session(lastModified = !!"DATETIME('now')")
      where ip == %arg["ip"] and password == %arg["pw"]
  client:
    proc updateSession(arg: Session)
  server:
    let allSessions = query:
      select session(_)
    send(allSessions)
  client:
    type Session = ref object
    var gSessions: seq[Session]
    gSessions = recv()
    proc getAllSessions()
