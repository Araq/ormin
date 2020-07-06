import times, strutils, sequtils, algorithm
import ormin
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
  var messages = newSeq[Message]()
  if usernames.len == 0: return
  for name in usernames:
    let res = query:
      select message(username, time, msg)
      where username == ?name
    messages &= res
  if messages.len > limit: 
    return messages.sortedByIt(it.time)[0 ..< limit]
  else:
    return messages.sortedByIt(it.time)