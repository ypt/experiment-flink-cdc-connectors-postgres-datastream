CREATE SCHEMA schema1;

CREATE TABLE schema1.users (
  id BIGSERIAL PRIMARY KEY,
  full_name VARCHAR
);

-- NOTE: REPLICA IDENDITY needs to be set to FULL
-- https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/formats/debezium.html#consuming-data-
-- https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-replica-identity
ALTER TABLE schema1.users REPLICA IDENTITY FULL;

CREATE TABLE schema1.user_favorite_colors (
  user_id BIGSERIAL PRIMARY KEY,
  favorite_color VARCHAR
);
ALTER TABLE schema1.user_favorite_colors REPLICA IDENTITY FULL;


CREATE SCHEMA schema2;

CREATE TABLE schema2.users (
  id BIGSERIAL PRIMARY KEY,
  full_name VARCHAR
);
ALTER TABLE schema2.users REPLICA IDENTITY FULL;

CREATE TABLE schema2.user_favorite_colors (
  user_id BIGSERIAL PRIMARY KEY,
  favorite_color VARCHAR
);
ALTER TABLE schema2.user_favorite_colors REPLICA IDENTITY FULL;
