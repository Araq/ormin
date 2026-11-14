create table records (
  id bigint primary key,
  public_id uuid not null,
  payload jsonb not null,
  amount double precision not null,
  exact_amount numeric(24, 6),
  tags text[] not null,
  note text
);
