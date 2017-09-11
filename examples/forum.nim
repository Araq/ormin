
staticExec("ormin_importer forum.sql --out:model.nim --db:sqlite")

include model
include ormin

rowq threadExists:
  thread: id
  where id == ?id

deleteq delAntibot:
  antibot
  where ip == ?ip

insertq insAntibot:
  antibot(?ip, ?answer)

rowq antibotAnswer:
  antibot: answer
  where ip == ?ip

insertq insPerson:
  person(?name, ?password, ?email, ?salt, status = $EmailUnconfirmed,
         lastOnline = r"DATETIME('now')")

deleteq logout:
  session
  where ip == ?ip and password == ?pw

updateq updateSession:
  session(lastModified = r"DATETIME('now')")
  where ip == ?ip and password == ?pw

rowq getUserId:
  session: (userId)
  where ip == ?ip and password == ?pw

rowq getUserdata:
  person: (name, email, status, ban)
  where id == ?id

updateq updateLastOnline:
  person(lastOnline = r"DATETIME('now')")
  where id == ?id

updateq incrementView:
  thread(views = views + 1)
  where id == ?id

rowq checkOwnership:
  post(author)
  where id == ?id

deleteq removeThreadWithoutPosts:
  thread
  where id notin (select post(thread))

updateq updateThreadModified:
  thread(modified = (select post(creation) where post.thread == ?thread
    orderby creation desc limit 1 ))

when false:
  # Check if post is the first post of the thread.
  let rows = db.getAllRows(sql("select id, thread, creation from post " &
        "where thread = ? order by creation asc"), $c.threadId)

  proc rateLimitCheck(c: var TForumData): bool =
    const query40 =
      sql("SELECT count(*) FROM post where author = ? and " &
          "(strftime('%s', 'now') - strftime('%s', creation)) < 40")
    const query90 =
      sql("SELECT count(*) FROM post where author = ? and " &
          "(strftime('%s', 'now') - strftime('%s', creation)) < 90")
    const query300 =
      sql("SELECT count(*) FROM post where author = ? and " &
          "(strftime('%s', 'now') - strftime('%s', creation)) < 300")
    # TODO Why can't I pass the secs as a param?
    let last40s = getValue(db, query40, c.userId).parseInt
    let last90s = getValue(db, query90, c.userId).parseInt
    let last300s = getValue(db, query300, c.userId).parseInt
    if last40s > 1: return true
    if last90s > 2: return true
    if last300s > 6: return true
    return false

  const query = sql"insert into thread(name, views, modified) values (?, 0, DATETIME('now'))"

  const query =
    sql"select id, name, password, email, salt, status, ban from person where name = ?"

      sql"insert into session (ip, password, userid) values (?, ?, ?)",
  const query =
    sql"select password, salt, strftime('%s', lastOnline) from person where name = ?"
  proc deleteAll(c: var TForumData, nick: string): bool =
    const query =
      sql("delete from post where author = (select id from person where name = ?)")

      sql("update person set status = ?, ban = ? where name = ?")

      sql("update person set password = ?, salt = ? where name = ?")

  proc getStats(c: var TForumData, simple: bool): TForumStats =
    const totalUsersQuery =
      sql"select count(*) from person"
    result.totalUsers = getValue(db, totalUsersQuery).parseInt
    const totalPostsQuery =
      sql"select count(*) from post"
    result.totalPosts = getValue(db, totalPostsQuery).parseInt
    const totalThreadsQuery =
      sql"select count(*) from thread"
    result.totalThreads = getValue(db, totalThreadsQuery).parseInt
    if not simple:
      var newestMemberCreation = 0
      result.activeUsers = @[]
      result.newestMember = ("", -1)
      const getUsersQuery =
        sql"select id, name, strftime('%s', lastOnline), strftime('%s', creation) from person"

  proc gatherTotalPostsByID(c: var TForumData, thrid: int): int =
    ## Gets the total post count of a thread.
    result = getValue(db, sql"select count(*) from post where thread = ?", $thrid).parseInt

  proc gatherTotalPosts(c: var TForumData) =
    if c.totalPosts > 0: return
    # Gather some data.
    const totalPostsQuery =
        sql"select count(*) from post p, person u where u.id = p.author and p.thread = ?"
    c.totalPosts = getValue(db, totalPostsQuery, $c.threadId).parseInt

  proc getThreadTitle(thrid: int, pageNum: int): string =
    result = getValue(db, sql"select name from thread where id = ?", $thrid)
  const getUIDQuery = sql"select id from person where name = ?"
  var uid = getValue(db, getUIDQuery, nick)
  if uid == "": return false
  result = true
  const totalPostsQuery =
      sql"select count(*) from post where author = ?"
  ui.posts = getValue(db, totalPostsQuery, uid).parseInt
  const totalThreadsQuery =
      sql("select count(*) from thread where id in (select thread from post where" &
         " author = ? and post.id in (select min(id) from post group by thread))")

  const lastOnlineQuery =
      sql"""select strftime('%s', lastOnline), email, ban, status
            from person where id = ?"""
