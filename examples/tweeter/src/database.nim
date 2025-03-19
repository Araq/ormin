import times, strutils, strformat, sequtils
from db_connector/db_sqlite import instantRows, `[]`
import ../../../ormin
import model

importModel(DbBackend.sqlite, "tweeter_model")
var db {.global.} = open("tweeter.db", "", "", "")

proc findUser*(username: string, user: var User): bool =
  let res = query:
    select user(username)
    where username == ?username
  echo res
  if res.len == 0: return false
  else: user.username = res[0]

  let following = query:
    select following(followed_user)
    where follower == ?username
  user.following = following.filterIt(it.len != 0)

  return true

proc create*(user: User) =
  query:
    insert user(username = ?user.username)

proc post*(message: Message) =
  if message.msg.len > 140:
    raise newException(ValueError, "Message has to be less than 140 characters.")
  query:
    insert message(username = ?message.username,
                   time = ?message.time,
                   msg = ?message.msg)

proc follow*(follower, user: User) =
  query:
    insert following(follower = ?follower.username, followed_user = ?user.username)

proc findMessage*(usernames: openArray[string], limit = 10): seq[Message] =
  result = @[]
  if usernames.len == 0: return
  var whereClause = "WHERE "
  for i in 0 ..< usernames.len:
    whereClause.add("trim(username) = ?")
    if i < usernames.len - 1:
      whereClause.add(" or ")

  let s = &"SELECT username, time, msg FROM Message {whereClause} ORDER BY time DESC LIMIT {limit}"
  for row in db.instantRows(sql(s), usernames):
    result.add((username: row[0], time: row[1].parseInt, msg: row[2]))
