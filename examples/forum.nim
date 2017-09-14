
import "../ormin"

importModel(DbBackend.sqlite, "examples", "forum_model")

let db = open("stuff", "", "", "")

const
  id = 90
  ip = "moo"
  answer = "dunno"
  pw = "mypw"
  email = "some@body.com"
  salt = "pepper"
  name = "me"

let thisThread = tryQuery:
  select thread(id)
  where id == ?id

createIter allThreadIds:
  select thread(id)
  where id == ?id

query:
  delete antibot
  where ip == ?ip

query:
  insert antibot(?ip, ?answer)

let something = query:
  select antibot(answer)
  where ip == ?ip
  orderby desc(ip)
  limit 1

query:
  insert person(?name, password = ?pw, ?email, ?salt, status = !!"'EmailUnconfirmed'",
         lastOnline = !!"DATETIME('now')")

query:
  delete session
  where ip == ?ip and password == ?pw

query:
  update session(lastModified = !!"DATETIME('now')")
  where ip == ?ip and password == ?pw

let userId1 = query:
  select session(userId)
  where ip == ?ip and password == ?pw

let (name9, email9, status, ban) = query:
  select person(name, email, status, ban)
  where id == ?id

query:
  update person(lastOnline = !!"DATETIME('now')")
  where id == ?id

query:
  update thread(views = views + 1, modified = !!"DATETIME('now')")
  where id == ?id

query:
  delete thread
  where id notin (select post(thread))

let (author, creation) = query:
  select post(author)
  join person(creation)

let (authorB, creationB) = query:
  select post(author)
  join person(creation) on author == id


#query:
#  update thread(modified = (select post(creation) where post.thread == ?thread
#    orderby creation desc limit 1 ))

#[
  # Check if post is the first post of the thread.
  let rows = db.getAllRows(sql("select id, thread, creation from post " &
        "where thread = ? order by creation asc"), $c.threadId)

  proc rateLimitCheck(c: var TForumData): bool =
    sql("SELECT count(*) FROM post where author = ? and " &
          "(strftime('%s', 'now') - strftime('%s', creation)) < 40")
    sql("SELECT count(*) FROM post where author = ? and " &
          "(strftime('%s', 'now') - strftime('%s', creation)) < 90")
    sql("SELECT count(*) FROM post where author = ? and " &
          "(strftime('%s', 'now') - strftime('%s', creation)) < 300")

  sql "insert into thread(name, views, modified) values (?, 0, DATETIME('now'))"
  sql "select id, name, password, email, salt, status, ban from person where name = ?"
  sql "insert into session (ip, password, userid) values (?, ?, ?)",
  sql"select password, salt, strftime('%s', lastOnline) from person where name = ?"
  sql("delete from post where author = (select id from person where name = ?)")
  sql("update person set status = ?, ban = ? where name = ?")
  sql("update person set password = ?, salt = ? where name = ?")
  sql"select count(*) from person"
  sql"select count(*) from post"
  sql"select count(*) from thread"
  sql"select id, name, strftime('%s', lastOnline), strftime('%s', creation) from person"

  sql"select count(*) from post where thread = ?"
  sql"select count(*) from post p, person u where u.id = p.author and p.thread = ?"
  sql"select name from thread where id = ?"
  sql"select id from person where name = ?"
  sql"select count(*) from post where author = ?"
  sql("select count(*) from thread where id in (select thread from post where" &
         " author = ? and post.id in (select min(id) from post group by thread))")
  sql"""select strftime('%s', lastOnline), email, ban, status
            from person where id = ?"""
]#
