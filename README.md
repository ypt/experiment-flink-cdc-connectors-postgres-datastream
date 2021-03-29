# experiment-flink-cdc-connectors-postgres-datastream
An experiment with Flink's [Debezium](https://debezium.io/) based
[flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors)
[DataStream
API](https://github.com/ververica/flink-cdc-connectors#usage-for-datastream-api).

## System
Here's the system this repo sets up

- Source: Two Postgres nodes
- → Change-data-capture via [flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors) (aka Flink 
  running embedded Debezium)
- → Flink stream processing logic
- → Sink: Stdout

## Run it
Bring up two Postgres nodes, each with two schemas.
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

Now examine your Flink application's stdout. You should see something like this.
If not, see [Troubleshooting](#troubleshooting).
```
SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23483376, lsn=23483376, txId=565, ts_usec=1617032465640931}} ConnectRecord{topic='postgres_cdc_source.schema1.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema1.users.Key:STRUCT}, value=Struct{after=Struct{id=1,full_name=susan smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1617032465640,db=experiment,schema=schema1,table=users,txId=565,lsn=23483376},op=c,ts_ms=1617032465709}, valueSchema=Schema{postgres_cdc_source.schema1.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23483752, lsn_commit=23483648, lsn=23483752, txId=566, ts_usec=1617032465645769}} ConnectRecord{topic='postgres_cdc_source.schema2.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema2.users.Key:STRUCT}, value=Struct{after=Struct{id=1,full_name=bob smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1617032465645,db=experiment,schema=schema2,table=users,txId=566,lsn=23483752},op=c,ts_ms=1617032465730}, valueSchema=Schema{postgres_cdc_source.schema2.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23484024, lsn_commit=23484024, lsn=23484024, txId=567, ts_usec=1617032465647414}} ConnectRecord{topic='postgres_cdc_source.schema1.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema1.users.Key:STRUCT}, value=Struct{before=Struct{id=1,full_name=susan smith},after=Struct{id=1,full_name=sue smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1617032465647,db=experiment,schema=schema1,table=users,txId=567,lsn=23484024},op=u,ts_ms=1617032465731}, valueSchema=Schema{postgres_cdc_source.schema1.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23484184, lsn_commit=23484184, lsn=23484184, txId=568, ts_usec=1617032466588937}} ConnectRecord{topic='postgres_cdc_source.schema2.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema2.users.Key:STRUCT}, value=Struct{before=Struct{id=1,full_name=bob smith},after=Struct{id=1,full_name=bobby smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1617032466588,db=experiment,schema=schema2,table=users,txId=568,lsn=23484184},op=u,ts_ms=1617032466753}, valueSchema=Schema{postgres_cdc_source.schema2.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
```

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

Your Flink application's stdout should display something like this:
```
SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23483376, lsn=23483376, txId=565, ts_usec=1617032554097436}} ConnectRecord{topic='postgres_cdc_source.schema3.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema3.users.Key:STRUCT}, value=Struct{after=Struct{id=1,full_name=anne smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1617032554097,db=experiment,schema=schema3,table=users,txId=565,lsn=23483376},op=c,ts_ms=1617032554373}, valueSchema=Schema{postgres_cdc_source.schema3.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23483752, lsn_commit=23483648, lsn=23483752, txId=566, ts_usec=1617032554105433}} ConnectRecord{topic='postgres_cdc_source.schema4.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema4.users.Key:STRUCT}, value=Struct{after=Struct{id=1,full_name=andy smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1617032554105,db=experiment,schema=schema4,table=users,txId=566,lsn=23483752},op=c,ts_ms=1617032554397}, valueSchema=Schema{postgres_cdc_source.schema4.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23484024, lsn_commit=23484024, lsn=23484024, txId=567, ts_usec=1617032554108097}} ConnectRecord{topic='postgres_cdc_source.schema3.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema3.users.Key:STRUCT}, value=Struct{before=Struct{id=1,full_name=anne smith},after=Struct{id=1,full_name=anna smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1617032554108,db=experiment,schema=schema3,table=users,txId=567,lsn=23484024},op=u,ts_ms=1617032554398}, valueSchema=Schema{postgres_cdc_source.schema3.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23484240, lsn_commit=23484184, lsn=23484240, txId=568, ts_usec=1617032554661678}} ConnectRecord{topic='postgres_cdc_source.schema4.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema4.users.Key:STRUCT}, value=Struct{before=Struct{id=1,full_name=andy smith},after=Struct{id=1,full_name=andrew smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1617032554661,db=experiment,schema=schema4,table=users,txId=568,lsn=23484240},op=u,ts_ms=1617032554907}, valueSchema=Schema{postgres_cdc_source.schema4.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
```

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