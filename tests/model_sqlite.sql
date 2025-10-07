create table if not exists tb_serial(
  typserial integer primary key,
  typinteger integer not null
);

create table if not exists tb_boolean(
  typboolean boolean not null
);

create table if not exists tb_float(
  typfloat real not null
);

create table if not exists tb_string(
  typstring text not null
);

create table if not exists tb_timestamp(
  dt1 timestamp not null,
  dt2 timestamp not null
);

create table if not exists tb_json(
  typjson json not null
);

create table if not exists tb_composite_pk(
  pk1 integer not null,
  pk2 integer not null,
  message text not null,
  primary key (pk1, pk2)
);

create table if not exists tb_composite_fk(
  id integer not null,
  fk1 integer not null,
  fk2 integer not null,
  foreign key (fk1, fk2) references tb_composite_pk(pk1, pk2)
);
create table if not exists tb_blob(
  id integer primary key,
  typblob blob not null
);
