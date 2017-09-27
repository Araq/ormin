
import "../ormin", json

importModel(DbBackend.sqlite, "examples", "forum_model")

var db {.global.} = open("stuff", "", "", "")

protocol "examples/forumclient.nim":
  common:
    when defined(js):
      type kstring = cstring
    else:
      type kstring = string
    type
      inet = kstring
      varchar = kstring
      timestamp = kstring
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
    gSessions = recv()
    proc getAllSessions()
