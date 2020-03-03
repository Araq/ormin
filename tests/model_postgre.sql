create table if not exists alltype(
  typserial serial primary key,
  typinteger integer not null,
  typbool boolean not null,
  typfloat real not null,
  typjson json not null,
  jsonstr varchar not null,
  typtimestamp timestamp not null default CURRENT_TIMESTAMP
);