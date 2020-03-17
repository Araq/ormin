create table if not exists tb_serial(
  typserial serial primary key,
  typinteger integer not null
);

create table if not exists tb_boolean(
  typboolean boolean not null
);

create table if not exists tb_float(
  typfloat float not null
);

create table if not exists tb_timestamp(
  dt timestamp not null,
  dtn timestamptz not null,
  dtz timestamptz not null
);