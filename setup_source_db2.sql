CREATE SCHEMA schema3;

CREATE TABLE schema3.users (
  id BIGSERIAL PRIMARY KEY,
  full_name VARCHAR
);

-- NOTE: REPLICA IDENDITY needs to be set to FULL
-- https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/formats/debezium.html#consuming-data-
-- https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-replica-identity
ALTER TABLE schema3.users REPLICA IDENTITY FULL;

CREATE TABLE schema3.user_favorite_colors (
  user_id BIGSERIAL PRIMARY KEY,
  favorite_color VARCHAR
);
ALTER TABLE schema3.user_favorite_colors REPLICA IDENTITY FULL;


CREATE SCHEMA schema4;

CREATE TABLE schema4.users (
  id BIGSERIAL PRIMARY KEY,
  full_name VARCHAR
);
ALTER TABLE schema4.users REPLICA IDENTITY FULL;

CREATE TABLE schema4.user_favorite_colors (
  user_id BIGSERIAL PRIMARY KEY,
  favorite_color VARCHAR
);
ALTER TABLE schema4.user_favorite_colors REPLICA IDENTITY FULL;

