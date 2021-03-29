CREATE SCHEMA schema3;

CREATE TABLE schema3.users (
  id BIGSERIAL PRIMARY KEY,
  full_name VARCHAR
);

-- NOTE: REPLICA IDENDITY needs to be set to FULL
-- https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/formats/debezium.html#consuming-data-
-- https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-replica-identity
ALTER TABLE schema3.users REPLICA IDENTITY FULL;


CREATE SCHEMA schema4;

CREATE TABLE schema4.users (
  id BIGSERIAL PRIMARY KEY,
  full_name VARCHAR
);

-- NOTE: REPLICA IDENDITY needs to be set to FULL
-- https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/formats/debezium.html#consuming-data-
-- https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-replica-identity
ALTER TABLE schema4.users REPLICA IDENTITY FULL;