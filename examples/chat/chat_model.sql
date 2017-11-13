create table if not exists users(
  id integer primary key,
  name varchar(20) not null,
  password varchar(32) not null,
  /* email varchar(30) not null, */
  creation timestamp not null default (DATETIME('now')),
  /* salt varchar(128) not null,
  status varchar(30) not null, */
  lastOnline timestamp not null default (DATETIME('now'))
);

/* Names need to be unique: */
create unique index if not exists UserNameIx on users(name);

create table if not exists messages(
  id integer primary key,
  author integer not null,
  /* ip inet not null, */
  content varchar(1000) not null,
  creation timestamp not null default (DATETIME('now')),

  foreign key (author) references users(id)
);
