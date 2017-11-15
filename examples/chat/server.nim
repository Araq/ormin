import ormin / [serverws]
import ormin

import json

# Ormin needs to know about our SQL model:
importModel(DbBackend.sqlite, "chat_model")

# Currently Ormin assumes a global database connection that needs to be
# annotated as '.global' for technical reasons. Later versions will improve.
var db {.global.} = open("chat.db", "", "", "")

# Ormin produces the file "chatclient.nim" for our frontend:
protocol "chatclient.nim":
  # A 'common' code section is shared by both the client and the server:
  common:
    when defined(js):
      type kstring = cstring
    else:
      type kstring = string
    type
      inet = kstring
      varchar = kstring
      timestamp = kstring

  # This is the initial request, it gives us the last 100 messages:
  server:
    let lastMessages = query:
      select messages(content, creation, author)
      join users(name)
      orderby desc(creation)
      limit 100
    send(lastMessages)
  client:
    type TextMessage* = ref object
    # Ormin fills in the fields of 'TextMessage' for us, based on the
    # query in the 'server' part.
    var allMessages*: seq[TextMessage] = @[]
    proc getRecentMessages*()
    allMessages = recv()

  # This is the request to send a new chat message:
  server:
    let userId = arg["author"].num
    # unregistered users cannot send anything:
    if userId == 0: return
    query:
      insert messages(content = %arg["content"], author = ?userId)
    query:
      update users(lastOnline = !!"DATETIME('now')")
      where id == ?userId

    let lastMessage = query:
      select messages(content, creation, author)
      join users(name)
      orderby desc(creation)
      limit 1

    receivers = Receivers.all
    send(lastMessage)

  client:
    proc sendMessage*(m: TextMessage)
    allMessages.add recv(TextMessage)

  # This is the request to register/login a new user:
  server:
    var candidates = query:
      produce nim
      select users(id, password)
      where name == %arg["name"]
    if candidates.len == 0:
      query:
        insert users(name = %arg["name"], password = %arg["password"])
      let userId = query:
        produce nim
        select users(id)
        where name == %arg["name"] and password == %arg["password"]
        limit 1
      send(%userId)
    else:
      block search:
        for c in candidates:
          if c[1] == arg["password"].str:
            send(%c[0])
            echo "login found!"
            break search
        # invalid login:
        send(%0)
        echo "no such user!"
  client:
    proc registerUser*(u: User)
    var userId: int
    userId = recv()
    if userId == 0: setError(username, "invalid login!")


serve "orminchat", dispatch
