DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test') THEN
      CREATE ROLE test LOGIN PASSWORD 'test';
   END IF;
END
$$;

DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'test') THEN
      CREATE DATABASE test OWNER test;
   END IF;
END
$$;

\connect test
GRANT ALL PRIVILEGES ON SCHEMA public TO test;

