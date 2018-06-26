-- uncomment sequence creation and alteration for before db creation
-- then comment again
  
-- create sequence users_id_seq;  
create table users(
  id integer primary key DEFAULT nextval('users_id_seq'),
  name varchar(20) not null,
  password varchar(32) not null,
  creation timestamp not null,
  lastOnline timestamp not null
);

-- alter sequence users_id_seq owned by users.id;
  
/* Names need to be unique: */
create unique index UserNameIx on users(name);

-- create sequence messages_id_seq;
create table if not exists messages(
  id integer primary key DEFAULT nextval('messages_id_seq'),
  author integer not null,
  content varchar(1000) not null,
  creation timestamp not null,

  foreign key (author) references users(id)
);

-- alter sequence messages_id_seq owned by messages.id;

