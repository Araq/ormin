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