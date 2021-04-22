package com.example.experimentflinkcdcconnectorspostgresdatastream;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // Typically, env can be set up this way if you don't care to bring up the web UI locally. The
        // getExecutionEnvironment() function will return the appropriate env depending on context of execution
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // However for easy local experimentation, we can explicitly specify a local streaming execution environment,
        // and also bring up a Web UI and REST endpoint - available at: http://localhost:8081
        //
        // Do NOT do this when actually packaging for deployment. Instead, just use getExecutionEnvironment()
        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
        conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // For more builder options, see:
        // https://github.com/ververica/flink-cdc-connectors/blob/master/flink-connector-postgres-cdc/src/main/java/com/alibaba/ververica/cdc/connectors/postgres/PostgreSQLSource.java#L43
        SourceFunction<String> sourceFunction1 = PostgreSQLSource.<String>builder()
                // The pgoutput logical decoding plugin is only supported in Postgres 10+
                // https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-pgoutput
                .decodingPluginName("pgoutput")

                // Slot names should be unique per Postgres node
                .slotName("flink1")

                .hostname("localhost")
                .port(5432)
                .database("experiment")
                .username("experiment")
                .password("experiment")

                // This simple deserializer just toString's a SourceRecord. We'll want to impl our own to extract the
                // data that we want.
                // See: https://github.com/ververica/flink-cdc-connectors/blob/release-1.2/flink-connector-debezium/src/main/java/com/alibaba/ververica/cdc/debezium/StringDebeziumDeserializationSchema.java
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        SourceFunction<String> sourceFunction2 = PostgreSQLSource.<String>builder()
                .decodingPluginName("pgoutput")
                .slotName("flink1")
                .hostname("localhost")
                .port(5433)
                .database("experiment")
                .username("experiment")
                .password("experiment")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        DataStream<String> stream1 = env.addSource(sourceFunction1);
        DataStream<String> stream2 = env.addSource(sourceFunction2);

        stream1
                .union(stream2)
                .print()
                .setParallelism(1);

        // Example output from the above
        // SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23469304, lsn=23469304, txId=565, ts_usec=1616805436265270}} ConnectRecord{topic='postgres_cdc_source.schema1.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema1.users.Key:STRUCT}, value=Struct{after=Struct{id=1,full_name=susan smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1616805436265,db=experiment,schema=schema1,table=users,txId=565,lsn=23469304},op=c,ts_ms=1616805436428}, valueSchema=Schema{postgres_cdc_source.schema1.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        // SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23469736, lsn_commit=23469576, lsn=23469736, txId=566, ts_usec=1616805450816777}} ConnectRecord{topic='postgres_cdc_source.schema2.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema2.users.Key:STRUCT}, value=Struct{after=Struct{id=1,full_name=bob smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1616805450816,db=experiment,schema=schema2,table=users,txId=566,lsn=23469736},op=c,ts_ms=1616805451269}, valueSchema=Schema{postgres_cdc_source.schema2.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        // SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23470064, lsn_commit=23470008, lsn=23470064, txId=567, ts_usec=1616805527734824}} ConnectRecord{topic='postgres_cdc_source.schema1.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema1.users.Key:STRUCT}, value=Struct{before=Struct{id=1,full_name=susan smith},after=Struct{id=1,full_name=sue smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1616805527734,db=experiment,schema=schema1,table=users,txId=567,lsn=23470064},op=u,ts_ms=1616805527737}, valueSchema=Schema{postgres_cdc_source.schema1.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        // SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23470304, lsn_commit=23470248, lsn=23470304, txId=568, ts_usec=1616805535802803}} ConnectRecord{topic='postgres_cdc_source.schema2.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema2.users.Key:STRUCT}, value=Struct{before=Struct{id=1,full_name=bob smith},after=Struct{id=1,full_name=bobby smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1616805535802,db=experiment,schema=schema2,table=users,txId=568,lsn=23470304},op=u,ts_ms=1616805535901}, valueSchema=Schema{postgres_cdc_source.schema2.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

        // Here's another example with some more operators that perform transformations & processing - the standard
        // word count example.
        // SingleOutputStreamOperator<Tuple2<String, Integer>> stream3 = stream1.flatMap(new Splitter());
        // SingleOutputStreamOperator<Tuple2<String, Integer>> stream4 = stream2.flatMap(new Splitter());
        //
        // stream3
        //         .union(stream4)
        //         .keyBy(value -> value.f0)
        //         .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        //         .sum(1)
        //         .print()
        //         .setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("experiment");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}