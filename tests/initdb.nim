import strutils, strformat, random
import ../ormin

let db {.global.} = when defined postgre:
  from db_postgres import exec, getValue
  const backend = DbBackend.postgre
  importModel(backend, "forum_model_postgres")
  open("localhost", "test", "test", "test")
else:
  from db_sqlite import exec, getValue
  const backend = DbBackend.sqlite
  importModel(backend, "forum_model_sqlite")
  open("tests/test.db", "", "", "")

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