import std/[assertions, strutils]

import ormin/importer_core

const supabaseSchema = """
do $$
begin
  create type public.service_client_kind as enum (
    'device',
    'internal_service',
    'partner'
  );
exception
  when duplicate_object then null;
end $$;

do $$
begin
  create type public.service_resource_kind as enum (
    'platform',
    'organization',
    'device',
    'integration'
  );
exception
  when duplicate_object then null;
end $$;

create table if not exists public.service_clients (
  id uuid primary key default gen_random_uuid(),
  client_id text not null unique
    check (client_id ~ '^[A-Za-z0-9._:-]{8,128}$'),
  kind public.service_client_kind not null,
  enabled boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

insert into public.service_clients (client_id, kind)
values
  ('device-001', 'device'),
  ('partner-001', 'partner')
on conflict (client_id) do nothing;

create table if not exists public.service_client_resource_grants (
  id bigint generated always as identity primary key,
  service_client_id uuid not null
    references public.service_clients(id) on delete cascade,
  resource_kind public.service_resource_kind not null,
  organization_id uuid,
  device_id text,
  integration text,
  resource_key text generated always as (
    case resource_kind
      when 'platform'::public.service_resource_kind then 'platform'
      when 'organization'::public.service_resource_kind then organization_id::text
      when 'device'::public.service_resource_kind then organization_id::text || ':' || device_id
      when 'integration'::public.service_resource_kind then lower(integration)
    end
  ) stored,
  constraint service_client_resource_grants_shape check (
    resource_kind = 'platform'::public.service_resource_kind
    or resource_kind = 'device'::public.service_resource_kind
  )
);

create index if not exists idx_service_client_resource_grants_device
  on public.service_client_resource_grants(organization_id, device_id)
  where device_id is not null;

drop trigger if exists set_service_clients_updated_at on public.service_clients;
create trigger set_service_clients_updated_at
before update on public.service_clients
for each row execute function public.set_updated_at();
"""

let schema = supabaseSchema
let model = generateModelCode(schema, "supabase_schema.sql", postgre)

doAssert model.contains("\"service_clients\"")
doAssert model.contains("\"service_client_resource_grants\"")
doAssert model.contains("Attr(name: \"id\", tabIndex: 0, typ: dbUuid, key: 1)")
doAssert model.contains("Attr(name: \"kind\", tabIndex: 0, typ: dbEnum, key: 0)")
doAssert model.contains("Attr(name: \"metadata\", tabIndex: 0, typ: dbJson, key: 0)")
doAssert model.contains("Attr(name: \"id\", tabIndex: 1, typ: dbInt, key: 1)")
doAssert model.contains("Attr(name: \"resource_kind\", tabIndex: 1, typ: dbEnum, key: 0)")
doAssert model.contains("Attr(name: \"resource_key\", tabIndex: 1, typ: dbVarchar, key: 0)")
