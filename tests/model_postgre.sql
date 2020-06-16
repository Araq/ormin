create table if not exists tb_serial(
  typserial serial primary key,
  typinteger integer not null
);

create table if not exists tb_boolean(
  typboolean boolean not null
);

create table if not exists tb_float(
  typfloat real not null
);

create table if not exists tb_string(
  typstring varchar not null
);

create table if not exists tb_timestamp(
  dt timestamp not null,
  dtn timestamptz not null,
  dtz timestamptz not null
);

create table if not exists tb_json(
  typjson json not null
);