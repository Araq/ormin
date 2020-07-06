import asyncdispatch, json, times
import jester, ormin
import views/[user, general]

importModel(DbBackend.sqlite, "tweeter_model")
var db {.global.} = open("tweeter.db", "", "", "")

type
  User* = object
    username*: string
    following*: seq[string]

  Message* = object
    username*: string
    time*: Time
    msg*: string

proc userLogin(request: Request, username: var string): bool =
  if request.cookies.hasKey("username"):
    let res = query:
      select user(username)
      where username == ?username
    if res.len == 0:
      query:
        insert user(username = ?username)
    return true
  else:
    return false

routes:
  get "/":
    var username: string
    if userLogin(request, username):
      let messages = query:
        select message(username, time, msg)
        where username == ?username
        produce json
      resp renderMain(renderTimeline(username, messages))
    else:
      resp renderMain(renderLogin())

  post "/login":
    setCookie("username", @"username", getTime().utc() + 2.hours)
    redirect "/"

  post "/createMessage":
    let
      username = @"username"
      message = @"message"
      time = getTime().toUnix().int
    query:
      insert message(username = ?username,
                     time = ?time,
                     msg = ?message)
    redirect "/"

runForever()