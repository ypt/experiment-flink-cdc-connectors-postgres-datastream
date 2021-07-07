# Experiment with Flink Pulsar SQL Client

> Note: this is not quite working, yet. Currently running into a Flink task manager slot allocation issue.

In docker-compose.yaml, uncomment sql-client, jobmanager, taskmanager services.

Bring up the system
docker-compose build
docker-compose up

Flink SQL client
docker-compose exec sql-client ./sql-client.sh

CREATE TABLE users_from_pulsar (
`schema` STRING,
`id` INT,
`fullName` STRING,
`op` STRING,
`eventTime` TIMESTAMP(3) METADATA,
`properties` MAP<STRING, STRING> METADATA ,
`topic` STRING METADATA VIRTUAL,
`sequenceId` BIGINT METADATA VIRTUAL,
PRIMARY KEY (schema, id) NOT ENFORCED
) WITH (
'connector' = 'upsert-pulsar',
'topic' = 'persistent://public/default/users',
'key.format' = 'json',
'value.format' = 'json',
'service-url' = 'pulsar://pulsar:6650',
'admin-url' = 'http://pulsar:8080'
);

SELECT * FROM users_from_pulsar;
