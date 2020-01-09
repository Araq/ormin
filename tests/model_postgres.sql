create table if not exists users(
  id integer primary key ,
  name varchar(20) not null,
  password varchar(32) not null,
  creation timestamp not null default CURRENT_TIMESTAMP,
  lastOnline timestamp not null default CURRENT_TIMESTAMP
);

create table if not exists messages(
  id integer primary key,
  author integer not null,
  content varchar(1000) not null,
  creation timestamp not null default CURRENT_TIMESTAMP,

  foreign key (author) references users(id)
);