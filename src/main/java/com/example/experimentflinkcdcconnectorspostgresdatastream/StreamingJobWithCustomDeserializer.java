package com.example.experimentflinkcdcconnectorspostgresdatastream;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

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
public class StreamingJobWithCustomDeserializer {
    public static final String TABLE_USERS = "users";
    public static final String TABLE_USER_FAVORITE_COLORS = "user_favorite_colors";

    //Side Output Example
    //https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/sideoutput/SideOutputExample.java

    private static final OutputTag<UsersEvent> usersEventOutputTag = new OutputTag<UsersEvent>("users-event-side-output") {
    };
    private static final OutputTag<UserFavoriteColorsEvent> userFavoriteColorsEventOutputTag = new OutputTag<UserFavoriteColorsEvent>("user-favorite-color-event-side-output") {
    };

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
        SourceFunction<DebeziumEvent> sourceFunction1 = PostgreSQLSource.<DebeziumEvent>builder()
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
                .deserializer(new MyDebeziumDeserializationSchema())
                .build();

        SourceFunction<DebeziumEvent> sourceFunction2 = PostgreSQLSource.<DebeziumEvent>builder()
                .decodingPluginName("pgoutput")
                .slotName("flink1")
                .hostname("localhost")
                .port(5433)
                .database("experiment")
                .username("experiment")
                .password("experiment")
                .deserializer(new MyDebeziumDeserializationSchema())
                .build();

        DataStream<DebeziumEvent> stream1 = env.addSource(sourceFunction1);
        DataStream<DebeziumEvent> stream2 = env.addSource(sourceFunction2);

        SingleOutputStreamOperator<DebeziumEvent> stream3 = stream1
                .union(stream2)
                .keyBy(v -> v.schema)
                .process(new MyProcessFunction());

        DataStream<UsersEvent> usersEventSideOutputStream = stream3.getSideOutput(usersEventOutputTag);
        usersEventSideOutputStream.print().setParallelism(1);

        DataStream<UserFavoriteColorsEvent> userFavoriteColorsEventSideOutputStream = stream3.getSideOutput(userFavoriteColorsEventOutputTag);
        userFavoriteColorsEventSideOutputStream.print().setParallelism(1);

        // Example output from the above
        // UsersEvent{fullName='susan smith', id=1, op='r', schema='schema1', table='users'}
        // UserFavoriteColorsEvent{favoriteColor='red', userId=1, op='r', schema='schema1', table='user_favorite_colors'}
        // UsersEvent{fullName='bob smith', id=1, op='r', schema='schema2', table='users'}
        // UserFavoriteColorsEvent{favoriteColor='blue', userId=1, op='r', schema='schema2', table='user_favorite_colors'}
        // UserFavoriteColorsEvent{favoriteColor='purple', userId=1, op='u', schema='schema1', table='user_favorite_colors'}

        // TODO: update readme
        // TODO: Stream -> Table API
        // TODO: write up single JDBC sink
        // TODO: demuxing to multiple sinks
        // TODO: write to Pulsar sink

        env.execute("experiment");
    }

    static class MyProcessFunction extends KeyedProcessFunction<String, DebeziumEvent, DebeziumEvent> {
        @Override
        public void processElement(DebeziumEvent value, Context ctx, Collector<DebeziumEvent> out) throws Exception {
            out.collect(value);

            switch (value.table) {
                case TABLE_USERS:
                    ctx.output(usersEventOutputTag, (UsersEvent) value);
                    break;
                case TABLE_USER_FAVORITE_COLORS:
                    ctx.output(userFavoriteColorsEventOutputTag, (UserFavoriteColorsEvent) value);

            }
        }
    }
}


class MyDebeziumDeserializationSchema implements DebeziumDeserializationSchema<DebeziumEvent> {
    private static final long serialVersionUID = -3168848963265670603L;
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_FAVORITE_COLOR = "favorite_color";
    private static final String FIELD_FULL_NAME = "full_name";
    private static final String FIELD_ID = "id";
    private static final String FIELD_OP = "op";
    private static final String FIELD_SCHEMA = "schema";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_TABLE = "table";
    private static final String FIELD_USER_ID = "user_id";

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<DebeziumEvent> collector) throws Exception {
        // System.out.println("RECORD: " + sourceRecord.toString());
        // Example output from the above
        // SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23469304, lsn=23469304, txId=565, ts_usec=1616805436265270}} ConnectRecord{topic='postgres_cdc_source.schema1.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema1.users.Key:STRUCT}, value=Struct{after=Struct{id=1,full_name=susan smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1616805436265,db=experiment,schema=schema1,table=users,txId=565,lsn=23469304},op=c,ts_ms=1616805436428}, valueSchema=Schema{postgres_cdc_source.schema1.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        // SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23469736, lsn_commit=23469576, lsn=23469736, txId=566, ts_usec=1616805450816777}} ConnectRecord{topic='postgres_cdc_source.schema2.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema2.users.Key:STRUCT}, value=Struct{after=Struct{id=1,full_name=bob smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1616805450816,db=experiment,schema=schema2,table=users,txId=566,lsn=23469736},op=c,ts_ms=1616805451269}, valueSchema=Schema{postgres_cdc_source.schema2.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        // SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23470064, lsn_commit=23470008, lsn=23470064, txId=567, ts_usec=1616805527734824}} ConnectRecord{topic='postgres_cdc_source.schema1.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema1.users.Key:STRUCT}, value=Struct{before=Struct{id=1,full_name=susan smith},after=Struct{id=1,full_name=sue smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1616805527734,db=experiment,schema=schema1,table=users,txId=567,lsn=23470064},op=u,ts_ms=1616805527737}, valueSchema=Schema{postgres_cdc_source.schema1.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        // SourceRecord{sourcePartition={server=postgres_cdc_source}, sourceOffset={transaction_id=null, lsn_proc=23470304, lsn_commit=23470248, lsn=23470304, txId=568, ts_usec=1616805535802803}} ConnectRecord{topic='postgres_cdc_source.schema2.users', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{postgres_cdc_source.schema2.users.Key:STRUCT}, value=Struct{before=Struct{id=1,full_name=bob smith},after=Struct{id=1,full_name=bobby smith},source=Struct{version=1.4.1.Final,connector=postgresql,name=postgres_cdc_source,ts_ms=1616805535802,db=experiment,schema=schema2,table=users,txId=568,lsn=23470304},op=u,ts_ms=1616805535901}, valueSchema=Schema{postgres_cdc_source.schema2.users.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

        Struct value = (Struct) sourceRecord.value();
        Struct source = (Struct) value.get(FIELD_SOURCE);
        Struct after = (Struct) value.get(FIELD_AFTER);
        String op = (String) value.get(FIELD_OP);

        switch (source.get(FIELD_TABLE).toString()) {
            case StreamingJobWithCustomDeserializer.TABLE_USERS:
                collector.collect(new UsersEvent(
                        op,
                        (String) source.get(FIELD_SCHEMA),
                        (Long) after.get(FIELD_ID),
                        (String) after.get(FIELD_FULL_NAME)
                ));
                break;
            case StreamingJobWithCustomDeserializer.TABLE_USER_FAVORITE_COLORS:
                collector.collect(new UserFavoriteColorsEvent(
                        op, (String)
                        source.get(FIELD_SCHEMA),
                        (Long) after.get(FIELD_USER_ID),
                        (String) after.get(FIELD_FAVORITE_COLOR)
                ));
                break;
        }
    }

    // https://ci.apache.org/projects/flink/flink-docs-stable/dev/types_serialization.html#flinks-typeinformation-class
    @Override
    public TypeInformation<DebeziumEvent> getProducedType() {
        return TypeExtractor.getForClass(DebeziumEvent.class);
    }
}

class UsersEvent extends DebeziumEvent {
    private final String fullName;
    private final Long id;

    public UsersEvent(
            String op,
            String schema,
            Long id,
            String fullName
    ) {
        super(op, schema, StreamingJobWithCustomDeserializer.TABLE_USERS);
        this.id = id;
        this.fullName = fullName;
    }

    @Override
    public String toString() {
        return "UsersEvent{" +
                "fullName='" + fullName + '\'' +
                ", id=" + id +
                ", op='" + op + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}

class UserFavoriteColorsEvent extends DebeziumEvent {
    private final String favoriteColor;
    private final Long userId;

    public UserFavoriteColorsEvent(
            String op,
            String schema,
            Long userId,
            String favoriteColor
    ) {
        super(op, schema, StreamingJobWithCustomDeserializer.TABLE_USER_FAVORITE_COLORS);
        this.userId = userId;
        this.favoriteColor = favoriteColor;
    }

    @Override
    public String toString() {
        return "UserFavoriteColorsEvent{" +
                "favoriteColor='" + favoriteColor + '\'' +
                ", userId=" + userId +
                ", op='" + op + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}

class DebeziumEvent {
    protected String op;
    protected String schema;
    protected String table;

    public DebeziumEvent(
            String op,
            String schema,
            String table
    ) {
        this.op = op;
        this.schema = schema;
        this.table = table;
    }

    @Override
    public String toString() {
        return "DebeziumEvent{" +
                "op='" + op + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
