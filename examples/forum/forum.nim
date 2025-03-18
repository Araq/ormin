import ../../ormin, json

importModel(DbBackend.sqlite, "forum_model")

var db {.global.} = open("stuff", "", "", "")

#var db: DbConn
#proc getPrepStmt(idx: int): PStmt

#var gPrepStmts: array[N, cstring]

type inetType = string

const
  id = 90
  ip = "moo"
  answer = "dunno"
  pw = "mypw"
  email = "some@body.com"
  salt = "pepper"
  name = "me"
  limit = 10
  offset = 5

let threads = query:
  select thread(id, name, views, modified)
  where id in (select post(thread) where author in
      (select person(id) where status notin ("Spammer") or id == ?id))
  orderby desc(modified)
  limit ?limit
  offset ?offset

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
  select antibot(answer & answer, (if ip == "hi": 0 else: 1))
  where ip == ?ip and answer =~ "%things%"
  orderby desc(ip)
  limit 1

let myNewPersonId: int = query:
  insert person(?name, password = ?pw, ?email, ?salt, status = !!"'EmailUnconfirmed'",
         lastOnline = !!"DATETIME('now')")
  returning id

query:
  delete session
  where ip == ?ip and password == ?pw

query:
  update session(lastModified = !!"DATETIME('now')")
  where ip == ?ip and password == ?pw

let myj = %*{"pw": "stuff here"}

let userId1 = query:
  select session(userId)
  where ip == ?ip and password == %myj["pw"]

let (name9, email9, status, ban) = query:
  select person(name, email, status, ban)
  where id == ?id
  limit 1

let (idg, nameg, pwg, emailg, creationg, saltg, statusg, lastOnlineg, bang) = query:
  select person(_)
  where id == ?id
  limit 1

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
  limit 1

let (authorB, creationB) = query:
  select post(author)
  join person(creation) on author == id
  limit 1

let allPosts = query:
  select post(count(_) as cnt)
  where cnt > 0
  produce json
  limit 1

createProc getAllThreadIds:
  select thread(id)
  where id == ?id
  produce json

let totalThreads = query:
  select thread(count(_))
  where id in (select post(thread) where author == ?id and id in (
    select post(min(id)) groupby thread))
  limit 1

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
