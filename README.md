# experiment-flink-cdc-connectors-postgres-datastream
An experiment with Flink's [Debezium](https://debezium.io/) based
[flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors)'s [DataStream
API](https://github.com/ververica/flink-cdc-connectors#usage-for-datastream-api)
and [Pulsar's Flink connector](https://github.com/streamnative/pulsar-flink).

## System
Here's the system this repo sets up

- Sources: Two Postgres nodes, each with two
  [schemas](https://www.postgresql.org/docs/10/ddl-schemas.html)
- → Change-data-capture via
  [flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors) (aka
  Flink running embedded Debezium)
- → Flink stream processing logic - selectively picking fields, merging tables
  logically
- → Sink: [Apache Pulsar](https://pulsar.apache.org/)

## Run it
Bring up the infrastructure, which includes:
- Two Postgres nodes, each with two
  [schemas](https://www.postgresql.org/docs/10/ddl-schemas.html)
- Apache Pulsar
```sh
docker-compose up
```

Run and test your application locally with an embedded instance of Flink
```sh
./gradlew run
```

The Flink web UI is also enabled locally. Visit http://localhost:8081.

Log into one of the Postgres nodes
```sh
docker-compose exec source-db1 psql experiment experiment
```

Examine replication slots, and insert and update some data
```sql
SELECT * FROM pg_replication_slots;

INSERT INTO schema1.users (full_name) VALUES ('susan smith');
INSERT INTO schema2.users (full_name) VALUES ('bob smith');
UPDATE schema1.users SET full_name = 'sue smith' where id = 1;
UPDATE schema2.users SET full_name = 'bobby smith' where id = 1;
```

Now examine your Flink application's `stdout`. You should see something like
this. If not, see [Troubleshooting](#troubleshooting).
```
UsersEvent{fullName='susan smith', id=1, op='c', schema='schema1', table='users'}
UsersEvent{fullName='bob smith', id=1, op='c', schema='schema2', table='users'}
UsersEvent{fullName='sue smith', id=1, op='u', schema='schema1', table='users'}
UsersEvent{fullName='bobby smith', id=1, op='u', schema='schema2', table='users'}
```

Now let's look for the same events in Pulsar, in the
`persistent://public/default/users` topic.

```sh
docker-compose exec pulsar ./bin/pulsar-client consume -s mysub2 -n 0 -p Earliest users

# ----- got message -----
# key:[null], properties:[], content:{"op":"c","schema":"schema1","table":"users","fullName":"susan smith","id":1}
# ----- got message -----
# key:[null], properties:[], content:{"op":"c","schema":"schema2","table":"users","fullName":"bob smith","id":1}
# ----- got message -----
# key:[null], properties:[], content:{"op":"u","schema":"schema1","table":"users","fullName":"sue smith","id":1}
# ----- got message -----
# key:[null], properties:[], content:{"op":"u","schema":"schema2","table":"users","fullName":"bobby smith","id":1}
```

Now let's try writing to our other Postgres node.

Log into the other Postgres node
```sh
docker-compose exec source-db2 psql experiment experiment
```

Examine replication slots, and insert and update some data
```sql
SELECT * FROM pg_replication_slots;

INSERT INTO schema3.users (full_name) VALUES ('anne smith');
INSERT INTO schema4.users (full_name) VALUES ('andy smith');
UPDATE schema3.users SET full_name = 'anna smith' where id = 1;
UPDATE schema4.users SET full_name = 'andrew smith' where id = 1;
```

Your Flink application's `stdout` should display something like this:
```
UsersEvent{fullName='anne smith', id=1, op='c', schema='schema3', table='users'}
UsersEvent{fullName='andy smith', id=1, op='c', schema='schema4', table='users'}
UsersEvent{fullName='anna smith', id=1, op='u', schema='schema3', table='users'}
UsersEvent{fullName='andrew smith', id=1, op='u', schema='schema4', table='users'}
```

Now take a look at your `pulsar-client` that is consuming the
`persistent://public/default/users` topic again. You should see the new changes
appear!

```sh
# ----- got message -----
# key:[null], properties:[], content:{"op":"c","schema":"schema3","table":"users","fullName":"anne smith","id":1}
# ----- got message -----
# key:[null], properties:[], content:{"op":"c","schema":"schema4","table":"users","fullName":"andy smith","id":1}
# ----- got message -----
# key:[null], properties:[], content:{"op":"u","schema":"schema3","table":"users","fullName":"anna smith","id":1}
# ----- got message -----
# key:[null], properties:[], content:{"op":"u","schema":"schema4","table":"users","fullName":"andrew smith","id":1}
```

## Recap
So what just happened?
- We started with two Postgres nodes as data sources, each with two
  [schemas](https://www.postgresql.org/docs/10/ddl-schemas.html).
- Our Flink job captures changes from these via
  [flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors)
- Our Flink job selectively grabs column data from these change events
- Our Flink job merges the changes from all of these Postgres nodes x schemas to
  a single stream per table
- Our Flink job then writes the merged streams to Pulsar, one topic per table.

## Next up
- Some other Flink job can consume [from
  Pulsar](https://github.com/streamnative/pulsar-flink#table-environment) and
  write to some other sink - for example a [JDBC
  sink](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/jdbc.html)
  - which can act as a stand in for a data warehouse. For ease of use, one can
    consider creating these jobs via [Flink SQL
    Client](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html).

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