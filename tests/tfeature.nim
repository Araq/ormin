import unittest, strformat, sequtils, algorithm, sugar, json, tables, random
import os
import ormin
import ormin/db_utils
when NimVersion < "1.2.0": import ./compat


let testDir = currentSourcePath.parentDir()

when defined postgre:
  from db_connector/db_postgres import exec, getValue

  const backend = DbBackend.postgre
  importModel(backend, "forum_model_postgres")
  const sqlFileName = "forum_model_postgres.sql"
  let db {.global.} = open("localhost", "test", "test", "test")
else:
  from db_connector/db_sqlite import exec, getValue

  const backend = DbBackend.sqlite
  importModel(backend, "forum_model_sqlite")
  const sqlFileName = "forum_model_sqlite.sql"
  var memoryPath = testDir & "/" & ":memory:"
  let db {.global.} = open(memoryPath, "", "", "")

var sqlFilePath = testDir & "/" & sqlFileName

type
  Person = tuple[id: int,
                name: string,
                password: string,
                email: string,
                salt: string,
                status: string]
  Thread = tuple[id: int,
                name: string,
                views: int]
  Post = tuple[id: int,
              author: int,
              ip: string,
              header: string,
              content: string,
              thread: int]
  Antibot = tuple[id: int,
                  ip: string,
                  answer: string]

const
  personcount = 5
  threadcount = 5
  postcount = 10
  antibotcount = 5
var
  persondata: seq[Person]
  threaddata: seq[Thread]
  postdata: seq[Post]
  antibotdata: seq[Antibot]   


suite &"Test ormin features of {backend}":
  discard

suite "query":
  db.dropTable(sqlFilePath)
  db.createTable(sqlFilePath)

  # prepare data to insert  
  for i in 1..personcount:
    persondata.add((id: i,
                    name: fmt"john{i}",
                    password: fmt"pass{i}",
                    email: fmt"john{i}@mail.com",
                    salt: fmt"abcd{i}",
                    status: fmt"ok{i}"))

  for i in 1..threadcount:
    threaddata.add((id: i,
                    name: fmt"thread{i}",
                    views: i))

  for i in 1..postcount:
    postdata.add((id: i,
                  author: sample({1..personcount}),
                  ip: "",
                  header: fmt"title{i}",
                  content: fmt"content{i}",
                  thread: sample({1..threadcount})))

  for i in 1..antibotcount:
    antibotdata.add((id: i,
                    ip: "",
                    answer: fmt"answer{i}"))

  # insert data into database
  let
    insertperson = sql"insert into person (id, name, password, email, salt, status) values (?, ?, ?, ?, ?, ?)"
    insertthread = sql"insert into thread (id, name, views) values (?, ?, ?)"
    insertpost = sql"insert into post (id, author, ip, header, content, thread) values (?, ?, ?, ?, ?, ?)"
    insertantibot = sql"insert into antibot (id, ip, answer) values (?, ?, ?)"

  for p in persondata:
    db.exec(insertperson, p.id, p.name, p.password, p.email, p.salt, p.status)

  for t in threaddata:
    db.exec(insertthread, t.id, t.name, t.views)

  for p in postdata:
    db.exec(insertpost, p.id, p.author, p.ip, p.header, p.content, p.thread)

  for a in antibotdata:
    db.exec(insertantibot, a.id, a.ip, a.answer)

  # check data in database
  let personexpected = db.getValue(sql"select count(*) from person")
  assert personexpected == $personcount

  let threadexpected = db.getValue(sql"select count(*) from thread")
  assert threadexpected == $threadcount

  let postexpected = db.getValue(sql"select count(*) from post")
  assert postexpected == $postcount

  let antibotexpected = db.getValue(sql"select count(*) from antibot")
  assert antibotexpected == $antibotcount

  test "table":
    let res = query:
      select person(id, name, password, email, salt, status)
    check res == persondata

  test "all_column":
    let res = query:
      select person(_)
    check res.mapIt((it.id, it.name, it.password, it.email, it.salt, it.status)) == persondata
  
  test "column_alias":
    let res = query:
      select person(id as personid, name as personname)
    check res == persondata.mapIt((personid: it.id, personname: it.name))

  test "arithmetic":
    let res = query:
      select person(id * 4 / 2 + 2 - 1 as id)
    check res == persondata.mapIt(int(it.id * 4 / 2 + 2 - 1))

  test "comparison":
    let id = 1
    let res = query:
      select person(id, name, password, email, salt, status)
      where id == ?id
    check res == [persondata[id - 1]]

  test "comparison_ne":
    let id = 1
    let res = query:
      select person(id, name, password, email, salt, status)
      where id != ?id
    check res == persondata.filterIt(it.id != id)

  test "comparison_ge":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where id >= ?id
    check res == persondata.filterIt(it.id >= id)

  test "comparison_le":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where id <= ?id
    check res == persondata.filterIt(it.id <= id)

  test "comparison_gt":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where id > ?id
    check res == persondata.filterIt(it.id > id)

  test "comparison_lt":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where id < ?id
    check res == persondata.filterIt(it.id < id)

  test "logical_not":
    let id = 3
    let res = query:
      select person(id, name, password, email, salt, status)
      where not (id > ?id)
    check res == persondata.filterIt(it.id <= id)

  test "logical_and":
    let
      id1 = 2
      id2 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id >= ?id1 and id <= ?id2
    check res == persondata.filterIt(it.id >= id1 and it.id <= id2)

  test "logical_or":
    let
      id1 = 2
      id2 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id == ?id1 or id == ?id2
    check res == persondata.filterIt(it.id == id1 or it.id == id2)

  test "logical_complex":
    let
      id1 = 2
      id2 = 3
      id3 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id > ?id2 and (id == ?id1 or id == ?id3)
    check res == persondata.filterIt(it.id == id3)

  test "predicate_in":
    let
      id1 = 2
      id2 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id in ?id1 .. ?id2
    check res == persondata.filterIt(it.id in id1..id2)

  test "predicate_not_in":
    let
      id1 = 2
      id2 = 4
    let res = query:
      select person(id, name, password, email, salt, status)
      where id notin ?id1 .. ?id2
    check res == persondata.filterIt(it.id < id1 or it.id > id2)

  test "limit":
    let id = 1
    let res = query:
      select person(id, name, password, email, salt, status)
      limit 1
    static:
      echo "LIMIT TEST: ", typeof(res)
    check res == persondata[id - 1]

  test "offset":
    let res = query:
      select person(id, name)
      limit 2
      offset 2
    check res == persondata[2..3].mapIt((it.id, it.name))

  test "match_assignment":
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

  test "groupby":
    let res = query:
      select post(author, count(id))
      groupby author
    let counttable = postdata.mapIt(it.author).toCountTable()
    for (author, c) in res:
      check counttable[author] == c

  test "groupby_aggregate":
    let author = 3
    let res = query:
      select post(count(id))
      where author == ?author
      groupby author
    let c = postdata.mapIt(it.author).toCountTable()[author]
    check res == [c]

  test "orderby":
    let res = query:
      select person(id, name)
      orderby id
    check res == persondata.mapIt((it.id, it.name)).sortedByIt(it[0])

  test "orderby_asc":
    let res = query:
      select person(id, name)
      orderby asc(id)
    check res == persondata.mapIt((it.id, it.name)).sortedByIt(it[0])

  test "orderby_desc":
    let res = query:
      select person(id, name)
      orderby desc(id)
    let expected = persondata.mapIt((it.id, it.name))
                            .sorted((x, y) => system.cmp(x[0], y[0]), Descending) 
    check res == expected

  test "orderby_key_select":
    let res = query:
      select person(id as personid, name)
      orderby personid
    check res == persondata.mapIt((it.id, it.name)).sortedByIt(it[0])

  test "orderby_key_not_select":
    let res = query:
      select person(name)
      orderby id
    check res == persondata.sortedByIt(it.id).mapIt(it.name)

  test "orderby_mulitple":
    # test fix #30 Incorrect handling of multiple sort keys in orderby
    let res = query:
      select post(author, id)
      orderby author, desc(id)
    check res == postdata.mapIt((it.author, it.id))
                        .sorted((x, y) => system.cmp(x[1], y[1]), Descending)
                        .sortedByIt(it[0])

  test "having":
    let id = 4
    let res = query:
      select post(author, count(_) as count)
      groupby author
      having author == ?id
    let expected = collect(newSeq):
      for p in postdata.mapIt(it.author).toCountTable().pairs():
        if p[0] == id: p
    check res == expected

  test "having_aggregate":
    let countvalue = 2
    let res = query:
      select post(author, count(id) as count)
      groupby author
      having count(id) >= ?countvalue
    let expected = collect(newSeq):
      for p in postdata.mapIt(it.author).toCountTable().pairs():
        if p[1] >= countvalue: p
    check res.sortedByIt(it[0])  == expected.sortedByIt(it[0])

  test "having_complex":
    let
      authorid1 = 1
      authorid2 = 3
      countvalue = 2
    let res = query:
      select post(author, count(id) as count)
      groupby author
      having count(id) >= ?countvalue and (author == ?authorid1 or author == ?authorid2)
    let expected = collect(newSeq):
      for p in postdata.mapIt(it.author).toCountTable().pairs():
        if p[0] in [authorid1, authorid2] and p[1] >= countvalue: p
    check res == expected

  test "subquery":
    let res = query:
      select post(id)
      where author in
        (select person(id))
    check res.sortedByIt(it) == postdata.mapIt(it.id)

  test "subquery_nest2":
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

  test "subquery_nest3":
    let res = query:
      select thread(id)
      where id in (select post(thread) where author in
        (select person(id) where id in {1, 2}))
    check res == postdata.filterIt(it.author in [1, 2])
                        .mapIt(it.thread)
                        .sortedByIt(it)

  test "subquery_having":
    # feature for having in subquery
    let id = 4
    let res = query:
      select person(id)
      where id in (
        select post(author) groupby author having author == ?id)
    check res == @[id]
      
  test "complex_query_subquery_having":
    let
      id1 = 2
      id2 = 3
    let res = query:
      select thread(count(_))
      where id in (
        select post(thread) where (author == ?id1 or author == ?id2) and id in (
          select post(min(id)) groupby thread having min(id) > 3))
      limit 1

    let threadinpost = postdata.mapIt(it.thread).deduplicate().sortedByIt(it)
    var postminids: seq[int]    
    for id in threadinpost:
      let group = collect(newSeq):
        for p in postdata:
          if p.thread == id: p.id
      postminids.add(group.min())
    let threadids = postdata.filterIt(it.author in [id1, id2] and
                                    it.id in postminids.filterIt(it > 3))
                            .mapIt(it.thread)
    check res == threadids.len()

  test "auto_join":
    let postid = 1
    let res = query:
      select post(author)
      join person(name)
      where id == ?postid
    let (author, name) = res[0]
    check name == persondata[author - 1].name

  test "join_on":
    # test fix #29 Cannot handle join on condition correctly
    let postid = 1
    let res = query:
      select post(author)
      join person(name) on author == id
      where id == ?postid
    let (author, name) = res[0]
    check name == persondata[author - 1].name

  test "casewhen":
    # need more test, only number
    # has problem with string or expression in condition
    let res = query:
      select person(name, (if id == 1: 10 elif id == 2: 20 else: 30) as pid)
    check res == persondata.mapIt((
                    name: it.name,
                    pid: if it.id == 1: 10 elif it.id == 2: 20 else: 30))

  test "produce_json":
    # test fix #27 Error: type mismatch: got <typeof(nil)>
    let threadjson = query:
      select thread(id, name)
      produce json
    let expected = threaddata.map do (it: tuple) -> JsonNode:
      result = newJObject()
      result["id"] = %it.id
      result["name"] = %it.name
    check threadjson == %expected

  test "produce_nim":
    let threadnim = query:
      select thread(id, name)
      produce nim
    check threadnim == threaddata.mapIt((it.id, it.name))

  test "tryquery":
    # unknow use, only not raise dbError?
    let res = tryQuery:
      select thread(id)
    check res == threaddata.mapIt(it.id)

  test "createproc":
    createProc getAllThreadIds:
      select thread(id)
    check db.getAllThreadIds() == threaddata.mapIt(it.id)

  test "createiter":
    createIter allThreadIdsIter:
      select thread(id)
    let res = collect(newSeq):
      for id in db.allThreadIdsIter:
        id
    check res.sortedByIt(it) == threaddata.mapIt(it.id)

  test "update_concat_op":
    let
      id = 3
      name = persondata[id - 1].name
      appendstr = "updated"
      nameupdated = name & appendstr
    query:
      update person(name = name & ?appendstr)
      where id == ?id
    let res = query:
      select person(name)
      where id == ?id
    check res[0] == nameupdated

  test "delete_one":
    # test fix #14: delete not return value
    let id = 1
    query:
      delete antibot
      where id == ?id
    let res = query:
      select antibot(_)
      where id == ?id
    check res == []

  test "delete_all":
    query:
      delete antibot
    let res = query:
      select antibot(_)
    check res == []

  test "insert_return_id":
    # test fix #28 returning id fail under postgresql 
      let expectedid = 6
      let id = query:
        insert antibot(id = ?expectedid, ip = "", answer = "just insert")
        returning id
      check id == expectedid

  test "insert_return_answer":
    # test fix #28 returning id fail under postgresql 
      let expectedanswer = "just insert"
      let answer = query:
        insert antibot(id = 9, ip = "", answer = ?expectedanswer)
        returning answer
      check answer == expectedanswer

  test "insert_return_id_auto":
    # test fix #28 returning id fail under postgresql 
      let answer = query:
        insert antibot(ip = "", answer = "just another insert")
        returning id
      check answer == 10

  test "insert_returning_uuid":
    # test fix #28 returning id fail under postgresql 
      let expecteduuid = "123e4567-e89b-12d3-a456-426614174000"
      let uuid = query:
        insert error(uuid = ?expecteduuid, message = "just insert")
        returning uuid
      check uuid == expecteduuid

  test "where_json":
    let
      id = 1
      p = %*{"id": id}
    let res = query:
      select person(id)
      where id == %p["id"]
    check res == [id]

  test "insert_json":
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

  test "update_json":
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
  
  test "insert_rawsql":
    let id = 8
    query:
      insert antibot(id = ?id, ip = "", answer = !!"'raw sql'",
                    created = !!"CURRENT_TIMESTAMP")
    let res = query:
      select antibot(_)
      where id == ?id
    check res[0].answer == "raw sql"

  test "update_rawsql":
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
