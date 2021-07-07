# experiment-flink-cdc-connectors-postgres-datastream
An experiment with Flink's [Debezium](https://debezium.io/) based
[flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors)'s
[DataStream
API](https://github.com/ververica/flink-cdc-connectors#usage-for-datastream-api)
and [Pulsar's Flink connector](https://github.com/streamnative/pulsar-flink).

## System
Here's the system this repo sets up

- Source: One Postgres node, with two
  [schemas](https://www.postgresql.org/docs/10/ddl-schemas.html)
- → Change-data-capture via
  [flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors) (aka
  Flink running embedded Debezium)
- → Flink stream processing logic - selectively picking fields, merging tables
  logically
- → Sink: [Apache Pulsar](https://pulsar.apache.org/)

And
- Source: [Apache Pulsar](https://pulsar.apache.org/)
- → Flink streaming aggregation
- → Sink: Postgres aggregate results table

## Run it
Bring up the infrastructure, which includes:
- One Postgres node with
  [schemas](https://www.postgresql.org/docs/10/ddl-schemas.html), functioning as
  a source
- Another Postgres node, functioning as a sink
- Apache Pulsar, functioning both sink and source
```sh
docker-compose up
```

Run and test your application locally with an embedded instance of Flink
```sh
./gradlew run
```

The Flink web UI is also enabled locally. Visit http://localhost:8081.

### Getting data from Postgres → Pulsar

Log into our Postgres node functioning as a source
```sh
docker-compose exec source-db1 psql experiment experiment
```

Examine replication slots, and insert and update some data
```sql
SELECT * FROM pg_replication_slots;

INSERT INTO schema1.users (full_name) VALUES ('susan smith');
INSERT INTO schema1.users (full_name) VALUES ('anne smith');
INSERT INTO schema2.users (full_name) VALUES ('bob smith');
UPDATE schema1.users SET full_name = 'sue smith' where id = 1;
UPDATE schema2.users SET full_name = 'bobby smith' where id = 1;
```

Now examine your Flink application's `stdout`. You should see something like
this. If not, see [Troubleshooting](#troubleshooting).
```
UsersEvent{key='schema1|1', fullName='susan smith', id=1, op='c', schema='schema1', table='users'}
UsersEvent{key='schema1|2', fullName='anne smith', id=2, op='c', schema='schema1', table='users'}
UsersEvent{key='schema2|1', fullName='bob smith', id=1, op='c', schema='schema2', table='users'}
UsersEvent{key='schema1|1', fullName='sue smith', id=1, op='u', schema='schema1', table='users'}
UsersEvent{key='schema2|1', fullName='bobby smith', id=1, op='u', schema='schema2', table='users'}
```

Now let's look for the same events in Pulsar, in the
`persistent://public/default/users` topic.

```sh
docker-compose exec pulsar ./bin/pulsar-client consume -s mysub2 -n 0 -p Earliest users

# ----- got message -----
# key:[c2NoZW1hMXwx], properties:[], content:{"key":"schema1|1","op":"c","schema":"schema1","table":"users","fullName":"susan smith","id":1}
# ----- got message -----
# key:[c2NoZW1hMXwy], properties:[], content:{"key":"schema1|2","op":"c","schema":"schema1","table":"users","fullName":"anne smith","id":2}
# ----- got message -----
# key:[c2NoZW1hMnwx], properties:[], content:{"key":"schema2|1","op":"c","schema":"schema2","table":"users","fullName":"bob smith","id":1}
# ----- got message -----
# key:[c2NoZW1hMXwx], properties:[], content:{"key":"schema1|1","op":"u","schema":"schema1","table":"users","fullName":"sue smith","id":1}
# ----- got message -----
# key:[c2NoZW1hMnwx], properties:[], content:{"key":"schema2|1","op":"u","schema":"schema2","table":"users","fullName":"bobby smith","id":1}
```

### Getting data from Pulsar → Postgres

Now, let's take a look at our Postgres node functioning as a sink

```sh
docker-compose exec sink-db1 psql experiment experiment
```

For reference, our Flink job should be upserting aggregated data there, like so:

```sql
INSERT INTO user_count_by_pgschema (pgschema, user_count)
SELECT schema, COUNT(1) as user_count
FROM users_from_pulsar
GROUP BY schema
```

Check out the aggregated results
```sql
SELECT * FROM user_count_by_pgschema;

#  pgschema | user_count
# ----------+------------
#  schema1  |          2
#  schema2  |          1
```

Now try inserting and updating rows in the Postgres source and watch the
Postgres sink reflect updated results.

## Recap
So what just happened?

First, we captured changes from Postgres → Pulsar
- We started with one Postgres node with multiple Postgres
  [schemas](https://www.postgresql.org/docs/10/ddl-schemas.html) as a data
  source
- Our Flink job captures changes from these via
  [flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors)
- Our Flink job selectively grabs column data from these change events
- Our Flink job merges the changes from all of the Postgres schemas to a single
  stream per table
- Our Flink job then writes the merged streams to Pulsar, one topic per table.

Then, we computed and wrote aggregated data from Pulsar → Postgres
- Given our Postgres changes written onto Pulsar from the previous step...
- We leveraged the [pulsar-flink](https://github.com/streamnative/pulsar-flink)
  connector and Flink's SQL API to fetch data from Pulsar
- We leveraged Flink's SQL API to compute some aggregates
- We leveraged Flink's SQL API to write the aggregate results to a [JDBC
  sink](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/jdbc.html)

## Next steps
Try...
- deletes
- writing to different kinds of sinks

## Troubleshooting
If you see this, until [this
issue](https://github.com/ververica/flink-cdc-connectors/issues/10) is fixed,
you may need to switch your JDK to Java 8.

```
Caused by: java.lang.NoSuchMethodError: sun.misc.Unsafe.monitorEnter(Ljava/lang/Object;)V
        at com.alibaba.ververica.cdc.debezium.internal.DebeziumChangeConsumer.handleBatch(DebeziumChangeConsumer.java:118)
        at io.debezium.embedded.ConvertingEngineBuilder.lambda$notifying$2(ConvertingEngineBuilder.java:82)
        at io.debezium.embedded.EmbeddedEngine.run(EmbeddedEngine.java:812)
        at io.debezium.embedded.ConvertingEngineBuilder$2.run(ConvertingEngineBuilder.java:171)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:834)
```

[Installing JDK](https://github.com/AdoptOpenJDK/homebrew-openjdk)

[Switching between
JDK's](https://medium.com/@devkosal/switching-java-jdk-versions-on-macos-80bc868e686a)

List JDK's available on your computer
```sh
/usr/libexec/java_home -V
```

Switch to the desired version. For example, to switch to JDK 8:
```sh
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
```

For `11.0`, switch `1.8` with `11.0`

Check your JDK version
```sh
java -version
```

## Deploy
To package your job for submission to a Flink cluster:
```
./gradlew shadowJar
```

Then look for the jar to use in the `build/libs` folder.

## Flink project bootstrap
I bootstrapped this project via the template here:

https://ci.apache.org/projects/flink/flink-docs-stable/dev/project-configuration.html#gradle

Feel free to start from scratch and adapt your own.