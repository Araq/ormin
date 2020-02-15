import unittest, strformat, sequtils, algorithm, sugar, json, tables
import ../ormin

include initdb

suite fmt"test common of {backend}":
  test "query a table":
    let res = query:
      select person(id, name, password, email, salt, status)
    check res == persondata

  test "query with all column":
    let res = query:
      select person(_)
    check res.mapIt((it.id, it.name, it.password, it.email, it.salt, it.status)) == persondata
  
  test "query with column alias":
    let res = query:
      select person(id as personid, name as personname)
    check res == persondata.mapIt((personid: it.id, personname: it.name))

  test "query with arithmetic operator: + - * /":
    let res = query:
      select person(id * 4 / 2 + 2 - 1 as id)
    check res == persondata.mapIt(int(it.id * 4 / 2 + 2 - 1))

  test "query with comparison operator: equal, and bind variable":
    let id = 1
    let res = query:
      select person(id, name, password, email, salt, status)
      where id == ?id
    check res == [persondata[id - 1]]

  test "query with comparison operator: not equal":
    let id = 1
    let res = query:
      select person(id, name, password, email, salt, status)
      where id != ?id
    check res == persondata.filterIt(it.id != id)

  test "query with comparison operator: great equal":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where id >= ?id
    check res == persondata.filterIt(it.id >= id)

  test "query with comparison operator: less equal":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where id <= ?id
    check res == persondata.filterIt(it.id <= id)

  test "query with comparison operator: great than":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where id > ?id
    check res == persondata.filterIt(it.id > id)

  test "query with comparison operator: less than":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where id < ?id
    check res == persondata.filterIt(it.id < id)

  test "query with logical operator: not":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where not (id > ?id)
    check res == persondata.filterIt(it.id <= id)

  test "query with logical operator: and":
    let
      id1 = 2
      id2 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id >= ?id1 and id <= ?id2
    check res == persondata.filterIt(it.id >= id1 and it.id <= id2)

  test "query with logical operator: or":
    let
      id1 = 2
      id2 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id == ?id1 or id == ?id2
    check res == persondata.filterIt(it.id == id1 or it.id == id2)

  test "query with logical operator: complex conditions":
    let
      id1 = 2
      id2 = 3
      id3 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id > ?id2 and (id == ?id1 or id == ?id3)
    check res == persondata.filterIt(it.id == id3)

  test "query with predicate: in(between) range":
    let
      id1 = 2
      id2 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id in ?id1 .. ?id2
    check res == persondata.filterIt(it.id in id1..id2)

  test "query with predicate: not in(between) range":
    let
      id1 = 2
      id2 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id notin ?id1 .. ?id2
    check res == persondata.filterIt(it.id < id1 or it.id > id2)

  test "query with limit":
    let id = 1
    let res = query:
      select person(id, name, password, email, salt, status)
      limit 1
    check res == persondata[id - 1]

  test "query with offset":
    let res = query:
      select person(id, name)
      limit 2
      offset 2
    check res == persondata[2..3].mapIt((it.id, it.name))

  test "query result match assignment":
    let id = 1
    let (name, password, email, salt, status) = query:
      select person(name, password, email, salt, status)
      where id == ?id
      limit 1
    check:
      name == persondata[id - 1].name
      password == persondata[id - 1].password
      email == persondata[id - 1].email
      salt == persondata[id - 1].salt
      status == persondata[id - 1].status

  test "query with group by":
    let res = query:
      select post(author, count(id))
      groupby author
    let counttable = postdata.mapIt(it.author).toCountTable()
    for (author, c) in res:
      check counttable[author] == c

  test "query with groupby: column not in select":
    let author = 3
    let res = query:
      select post(count(id))
      where author == ?author
      groupby author
    let c = postdata.mapIt(it.author).toCountTable()[author]
    check res == [c]

  test "query with orderby: default":
    let res = query:
      select person(id, name)
      orderby id
    check res == persondata.mapIt((it.id, it.name)).sortedByIt(it[0])

  test "query with orderby: asc":
    let res = query:
      select person(id, name)
      orderby asc(id)
    check res == persondata.mapIt((it.id, it.name)).sortedByIt(it[0])

  test "query with orderby: desc":
    let res = query:
      select person(id, name)
      orderby desc(id)
    let expected = persondata.mapIt((it.id, it.name))
                             .sorted((x, y) => system.cmp(x[0], y[0]), Descending) 
    check res == expected

  test "query with orderby: key is column alias":
    let res = query:
      select person(id as personid, name)
      orderby personid
    check res == persondata.mapIt((it.id, it.name)).sortedByIt(it[0])

  test "query with orderby: column not in select":
    let res = query:
      select person(name)
      orderby id
    check res == persondata.sortedByIt(it.id).mapIt(it.name)

  test "query with having":
    let countvalue = 2
    let res = query:
      select post(author, count(id) as count)
      groupby author
      having count(id) >= ?countvalue
    let ct = postdata.mapIt(it.author).toCountTable()
    {.push warning[deprecated]: off.}
    let expected = lc[p | (p <- ct.pairs(), p[1] >= countvalue), tuple[author, count: int]]
    {.pop.}
    check res.sortedByIt(it.author) == expected

  test "query with having complex conditions":
    let
      authorid1 = 1
      authorid2 = 3
      countvalue = 2
    let res = query:
      select post(author, count(id) as count)
      groupby author
      having count(id) >= ?countvalue and (author == ?authorid1 or author == ?authorid2)
    let ct = postdata.mapIt(it.author).toCountTable()
    {.push warning[deprecated]: off.}
    let expected = lc[p | (p <- ct.pairs(), (p[0] in [authorid1, authorid2] and p[1] >= countvalue)), tuple[author, count: int]]
    {.pop.}
    check res == expected

  test "subquery with predicate in":
    let res = query:
      select post(id)
      where author in
        (select person(id))
    check res.sortedByIt(it) == postdata.mapIt(it.id)

  test "subquery Two-level nesting":
    let
      personid1 = 1
      personid2 = 2
      expectedpost = postdata.filterIt(it.author in [personid1, personid2])
                             .mapIt(it.id)
    let res = query:
      select post(id)
      where author in
        (select person(id) where id == ?personid1 or id == ?personid2)
    check res.sortedByIt(it) == expectedpost.sortedByIt(it)

  test "subquery Three-level nesting":
    let res = query:
      select thread(id)
      where id in (select post(thread) where author in
        (select person(id) where id in {1, 2}))
    check res == postdata.filterIt(it.author in [1, 2])
                         .mapIt(it.thread)
                         .sortedByIt(it)

  test "query with auto join":
    let postid = 1
    let res = query:
      select post(author)
      join person(name)
      where id == ?postid
    let (author, name) = res[0]
    check name == persondata[author - 1].name

  test "query with case when":
    # need more test, only number
    # has problem with string or expression in condition
    let res = query:
      select person(name, (if id == 1: 10 elif id == 2: 20 else: 30) as pid)
    check res == persondata.mapIt((
                    name: it.name,
                    pid: if it.id == 1: 10 elif it.id == 2: 20 else: 30))
      
  test "update value with ||":
    let
      id = 3
      name = persondata[id - 1].name
      appendstr = "updated"
      nameupdated = name & appendstr
    query:
      update person(name = name || ?appendstr)
      where id == ?id
    let res = query:
      select person(name)
      where id == ?id
    check res[0] == nameupdated

  test "delete one record":
    # test fix #14: delete not return value
    let id = 1
    query:
      delete antibot
      where id == ?id
    let res = query:
      select antibot(_)
      where id == ?id
    check res == []

  test "delete all data":
    query:
      delete antibot
    let res = query:
      select antibot(_)
    check res == []

  test "query with produce json":
    # test fix #27 Error: type mismatch: got <typeof(nil)>
    let threadjson = query:
      select thread(id, name)
      produce json
    let expected = threaddata.map do (it: tuple) -> JsonNode:
      result = newJObject()
      result["id"] = %it.id
      result["name"] = %it.name
    check threadjson == %expected

  test "query with default produce nim":
    let threadnim = query:
      select thread(id, name)
      produce nim
    check threadnim == threaddata.mapIt((it.id, it.name))

  test "tryQuery":
    # unknow use, only not raise dbError?
    let res = tryQuery:
      select thread(id)
    check res == threaddata.mapIt(it.id)

  test "createProc":
    createProc getAllThreadIds:
      select thread(id)
    check db.getAllThreadIds() == threaddata.mapIt(it.id)

  test "createIter":
    createIter allThreadIdsIter:
      select thread(id)
    {.push warning[deprecated]: off.}
    let res = lc[id | (id <- db.allThreadIdsIter), int]
    {.pop.}
    check res.sortedByIt(it) == threaddata.mapIt(it.id)

  test "query condition bind json object":
    let
      id = 1
      p = %*{"id": id}
    let res = query:
      select person(id)
      where id == %p["id"]
    check res == [id]

  test "insert with json object":
    let
      id = 7
      answer = "json answer"
      a = %*{"answer": answer}
    query:
      insert antibot(id = ?id, ip = "", answer = %a["answer"])
    let res = query:
      select antibot(answer)
      where id == ?id
    check res[0] == answer

  test "update with json object":
    let
      id = 3
      name = "json"
      p = %*{"name": name}
    query:
      update person(name = %p["name"])
      where id == ?id
    let res = query:
      select person(name)
      where id == ?id
    check res[0] == name
  
  test "insert with raw sql using: !!":
    let id = 8
    query:
      insert antibot(id = ?id, ip = "", answer = !!"'raw sql'",
                     created = !!"CURRENT_TIMESTAMP")
    let res = query:
      select antibot(_)
      where id == ?id
    check res[0].answer == "raw sql"

  test "update with raw sql using: !!":
    let id = 3
    let res = query:
      select person(name)
      where id == ?id
    query:
      update person(name = !!"UPPER(name)")
      where id == ?id
    let res2 = query:
      select person(name)
      where id == ?id
    check res[0] != res2[0]
    check res[0].toUpper() == res2[0]