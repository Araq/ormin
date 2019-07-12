import "./ormin.nim"
#import ormin
import json

importModel(DbBackend.sqlite, "chat_model")

var db {.global.} = open("chat.db", "", "", "")

let users = query:
  #produce json
  select users(id, name as username)

echo users
#
#import typetraits
#
#echo users.typedesc
#
#for user in users:
#  echo user.repr
#  echo user.username
#
#let messages = query:
#  select messages(id, content, creation)
#  join users(name as author)
#
#var maxId = 0
#echo messages
#for message in messages:
#  echo message.author, "\t", message.creation
#  echo "\t", message.content
#  maxId = max(maxId, message.id)
#
##maxId += 1
#
##query:
##  insert messages(id = ?maxId, author = 1, content = "Hello?")
#
##createIter allThreadIds:
##let lastmsg = query:
#createIter allMessageIds:
#  select messages(id)
##  where id == ?id
#
#for message in db.allMessageIds():
#  echo message.id
##echo lastmsg
#
#let thisThread = tryQuery:
#  select messages(id, author)
#  where id == ?maxId
#  limit 1
#
#echo thisThread
#
#let thisThread2 = query:
#  select messages(id, author)
#  where id == ?maxId
#  limit 1
#
#echo thisThread2
#
#createProc getAllMessageIds:
#  select messages(id)
#  where id == ?id
#  produce json
#
#echo db.getAllMessageIds(3)
#
#let allPosts = query:
#  select messages(count(id) as cnt)
#  produce json
#  limit 1
#
#echo allPosts

#let allFields = query:
#  select messages(_)

#echo allFields
