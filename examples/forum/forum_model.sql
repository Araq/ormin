
create table if not exists thread(
  id integer primary key,
  name varchar(100) not null,
  views integer not null,
  modified timestamp not null default (DATETIME('now'))
);

create unique index if not exists ThreadNameIx on thread (name);

create table if not exists person(
  id integer primary key,
  name varchar(20) not null,
  password varchar(32) not null,
  email varchar(30) not null,
  creation timestamp not null default (DATETIME('now')),
  salt varchar(128) not null,
  status varchar(30) not null,
  lastOnline timestamp not null default (DATETIME('now')),
  ban varchar(128) not null default ''
);

create unique index if not exists UserNameIx on person (name);

create table if not exists post(
  id integer primary key,
  author integer not null,
  ip inet not null,
  header varchar(100) not null,
  content varchar(1000) not null,
  thread integer not null,
  creation timestamp not null default (DATETIME('now')),

  foreign key (thread) references thread(id),
  foreign key (author) references person(id)
);

create table if not exists session(
  id integer primary key,
  ip inet not null,
  password varchar(32) not null,
  userid integer not null,
  lastModified timestamp not null default (DATETIME('now')),
  foreign key (userid) references person(id)
);

create table if not exists antibot(
  id integer primary key,
  ip inet not null,
  answer varchar(30) not null,
  created timestamp not null default (DATETIME('now'))
);

create index PersonStatusIdx on person(status);
create index PostByAuthorIdx on post(thread, author);
