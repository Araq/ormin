
import ormin
import json, times

importModel(DbBackend.postgre, "chat_model")

let db {.global.} = open("localhost", "ormin", "", "chatpg")

proc createtUser(name, pass: string) =
  echo "Creating user with name: $1" % name
  query:
    insert users(name = ?name, password = ?pass, creation = "now()", lastonline = "now()")

proc listAllUsers() =
  let users = query:
    select users(id, name, creation, lastonline)
  echo users

proc createMessage(userId: int, content, creation: string) =
  query:
    insert messages(content = ?content, author = ?userId, creation = ?creation)

proc getLastMessages() = 
  let lastMessages = query:
    select messages(content, creation, author)
    join users(name)
    orderby desc(creation)
    limit 100
  echo lastMessages
      
proc getLastMessage() =
  let lastMessage = query:
    select messages(content, creation, author)
    join users(name)
    orderby desc(creation)
    limit 1

proc updateUser(userId: int, lastOnline: string) =
  query:
    update users(lastOnline = ?lastOnline)
    where id == ?userId

# user creation
#createtUser("testuser", "testpass")
listAllUsers()

# # insert rows using datetime from nim
let
  d = now()
  userId = 1 # first created user should be 1

# createMessage(userId, "Inserted content", $d)
# createMessage(userId, "Another nserted content", $d)

# getLastMessages()
# getLastMessage()
updateUser(userId, $d)
