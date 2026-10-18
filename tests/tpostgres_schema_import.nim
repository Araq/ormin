import std/[assertions, strutils]

import ormin/importer_core

const postgresSchema = """
do $$
begin
  create type public.client_kind as enum (
    'device',
    'internal',
    'partner'
  );
exception
  when duplicate_object then null;
end $$;

do $$
begin
  create type public.resource_kind as enum (
    'platform',
    'organization',
    'device',
    'integration'
  );
exception
  when duplicate_object then null;
end $$;

create table if not exists public.clients (
  id uuid primary key default gen_random_uuid(),
  client_id text not null unique
    check (client_id ~ '^[A-Za-z0-9._:-]{8,128}$'),
  kind public.client_kind not null,
  enabled boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

insert into public.clients (client_id, kind)
values
  ('device-001', 'device'),
  ('partner-001', 'partner')
on conflict (client_id) do nothing;

create table if not exists public.client_resource_grants (
  id bigint generated always as identity primary key,
  service_client_id uuid not null
    references public.clients(id) on delete cascade,
  resource_kind public.resource_kind not null,
  organization_id uuid,
  device_id text,
  integration text,
  resource_key text generated always as (
    case resource_kind
      when 'platform'::public.resource_kind then 'platform'
      when 'organization'::public.resource_kind then organization_id::text
      when 'device'::public.resource_kind then organization_id::text || ':' || device_id
      when 'integration'::public.resource_kind then lower(integration)
    end
  ) stored,
  constraint client_resource_grants_shape check (
    resource_kind = 'platform'::public.resource_kind
    or resource_kind = 'device'::public.resource_kind
  )
);

create index if not exists idx_client_resource_grants_device
  on public.client_resource_grants(organization_id, device_id)
  where device_id is not null;

drop trigger if exists set_clients_updated_at on public.clients;
create trigger set_clients_updated_at
before update on public.clients
for each row execute function public.set_updated_at();
"""

let schema = postgresSchema
let model = generateModelCode(schema, "postgres_schema.sql", postgre)

doAssert model.contains("\"public.clients\"")
doAssert model.contains("\"public.client_resource_grants\"")
doAssert model.contains("Attr(name: \"id\", tabIndex: 0, typ: dbUuid")
doAssert model.contains("typeName: \"uuid\", validValues: @[], key: 1")
doAssert model.contains("Attr(name: \"kind\", tabIndex: 0, typ: dbEnum")
doAssert model.contains(
  "typeName: \"public.client_kind\", validValues: @[" &
    "\"device\", \"internal\", \"partner\"]"
)
doAssert model.contains("Attr(name: \"metadata\", tabIndex: 0, typ: dbJson")
doAssert model.contains("typeName: \"jsonb\", validValues: @[], key: 0")
doAssert model.contains("Attr(name: \"id\", tabIndex: 1, typ: dbInt")
doAssert model.contains("typeName: \"bigint\", validValues: @[], key: 1")
doAssert model.contains("Attr(name: \"resource_kind\", tabIndex: 1, typ: dbEnum")
doAssert model.contains(
  "typeName: \"public.resource_kind\", validValues: @[" &
    "\"platform\", \"organization\", \"device\", \"integration\"]"
)
doAssert model.contains("Attr(name: \"resource_key\", tabIndex: 1, typ: dbVarchar")

block qualifiedTableNamesRemainDistinct:
  const schemaText = """
create table public.events (public_value text);
create table audit.events (audit_value text);
"""
  let schema = schemaText
  let generated = generateModelCode(schema, "qualified.sql", postgre)
  doAssert generated.contains("\"public.events\"")
  doAssert generated.contains("\"audit.events\"")
  doAssert generated.contains("Attr(name: \"public_value\", tabIndex: 0")
  doAssert generated.contains("Attr(name: \"audit_value\", tabIndex: 1")

block enumKeywordsMayBeSeparatedByWhitespace:
  const schemaText = """
create
type public.mood as enum ('happy', 'sad');
create table public.people (mood public.mood);
"""
  let schema = schemaText
  let generated = generateModelCode(schema, "enum_whitespace.sql", postgre)
  doAssert generated.contains("Attr(name: \"mood\", tabIndex: 0, typ: dbEnum")
  doAssert generated.contains(
    "typeName: \"public.mood\", validValues: @[\"happy\", \"sad\"]"
  )

block namedUniqueConstraintImports:
  const schemaText = """
create table public.people (
  email text,
  constraint people_email_key unique (email)
);
"""
  let schema = schemaText
  let generated = generateModelCode(schema, "named_unique.sql", postgre)
  doAssert generated.contains("Attr(name: \"email\", tabIndex: 0, typ: dbVarchar")
