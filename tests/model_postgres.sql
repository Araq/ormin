create sequence if not exists users_id_seq;
create table if not exists users(
  id integer primary key default nextval('users_id_seq'),
  name varchar(20) not null,
  password varchar(32) not null,
  creation timestamp not null default CURRENT_TIMESTAMP,
  lastOnline timestamp not null default CURRENT_TIMESTAMP
);

alter sequence users_id_seq owned by users.id;

create sequence if not exists messages_id_seq;
create table if not exists messages(
  id integer primary key default nextval('messages_id_seq'),
  author integer not null,
  content varchar(1000) not null,
  creation timestamp not null default CURRENT_TIMESTAMP,

  foreign key (author) references users(id)
);

alter sequence messages_id_seq owned by messages.id;
