package com.example.experimentflinkcdcconnectorspostgresdatastream;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonSer;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;

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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // However for easy local experimentation, we can explicitly specify a local streaming execution environment,
        // and also bring up a Web UI and REST endpoint - available at: http://localhost:8081
        //
        // Do NOT do this when actually packaging for deployment. Instead, just use getExecutionEnvironment()
        // Configuration conf = new Configuration();
        // conf.setString("state.backend", "filesystem");
        // conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
        // conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
        // conf.setInteger(RestOptions.PORT, 8081);
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // Postgres -> Flink CDC -> Pulsar
        // -------------------------------

        // Postgres source

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

        DataStream<DebeziumEvent> cdcStream = env.addSource(sourceFunction1);

        SingleOutputStreamOperator<DebeziumEvent> streamWithSideOutputs = cdcStream
                .keyBy(v -> v.schema)
                .process(new MyProcessFunction());

        // Pulsar sinks

        Properties pulsarSinkProps = new Properties();
        ClientConfigurationData pulsarClientConf = new ClientConfigurationData();
        pulsarClientConf.setServiceUrl("pulsar://localhost:6650");

        // Pulsar sink for UsersEvent
        PulsarSerializationSchema<UsersEvent> pulsarUsersSerialization = new PulsarSerializationSchemaWrapper.Builder<>(JsonSer.of(UsersEvent.class))
                .usePojoMode(UsersEvent.class, RecordSchemaType.JSON)
                .setKeyExtractor(u -> u.key.getBytes(StandardCharsets.UTF_8))
                .build();
        FlinkPulsarSink<UsersEvent> pulsarUsersSink = new FlinkPulsarSink(
                "http://localhost:8080",
                Optional.of("persistent://public/default/users"), // mandatory target topic or use `Optional.empty()` if pulsarUsersSink to different topics for each record
                pulsarClientConf,
                pulsarSinkProps,
                pulsarUsersSerialization,
                PulsarSinkSemantic.AT_LEAST_ONCE
        );

        // Pulsar sink for UserFavoriteColorsEvent
        PulsarSerializationSchema<UserFavoriteColorsEvent> pulsarUserFavoriteColorsSerialization = new PulsarSerializationSchemaWrapper.Builder<>(JsonSer.of(UserFavoriteColorsEvent.class))
                .usePojoMode(UserFavoriteColorsEvent.class, RecordSchemaType.JSON)
                .setKeyExtractor(u -> u.key.getBytes(StandardCharsets.UTF_8))
                .build();
        FlinkPulsarSink<UserFavoriteColorsEvent> pulsarUserFavoriteColorsSink = new FlinkPulsarSink(
                "http://localhost:8080",
                Optional.of("persistent://public/default/user_favorite_colors"), // mandatory target topic or use `Optional.empty()` if pulsarUsersSink to different topics for each record
                pulsarClientConf,
                pulsarSinkProps,
                pulsarUserFavoriteColorsSerialization,
                PulsarSinkSemantic.AT_LEAST_ONCE
        );

        DataStream<UsersEvent> usersEventSideOutputStream = streamWithSideOutputs.getSideOutput(usersEventOutputTag);
        usersEventSideOutputStream.addSink(pulsarUsersSink).uid("pulsar_users_sink").name("pulsar_users_sink");
        usersEventSideOutputStream.print().setParallelism(1);

        DataStream<UserFavoriteColorsEvent> userFavoriteColorsEventSideOutputStream = streamWithSideOutputs.getSideOutput(userFavoriteColorsEventOutputTag);
        userFavoriteColorsEventSideOutputStream.addSink(pulsarUserFavoriteColorsSink).uid("pulsar_user_favorite_colors_sink").name("pulsar_user_favorite_colors_sink");
        userFavoriteColorsEventSideOutputStream.print().setParallelism(1);

        // Example output from the above print() streams
        // UsersEvent{key='schema1|1', fullName='susan smith', id=1, op='r', schema='schema1', table='users'}
        // UserFavoriteColorsEvent{key='schema1|1', favoriteColor='red', userId=1, op='r', schema='schema1', table='user_favorite_colors'}
        // UsersEvent{key='schema2|1', fullName='bob smith', id=1, op='r', schema='schema2', table='users'}
        // UserFavoriteColorsEvent{key='schema2|1', favoriteColor='blue', userId=1, op='r', schema='schema2', table='user_favorite_colors'}
        // UserFavoriteColorsEvent{key='schema1|1', favoriteColor='purple', userId=1, op='u', schema='schema1', table='user_favorite_colors'}


        // Pulsar source -> Postgres sink w/ Flink SQL API
        // -----------------------------------------------

        // Once the change data capture stream is in Pulsar, we can easily apply transformations and write to an
        // assortment of sinks in near real-time.
        //
        // Below, we use the higher level Flink SQL API to read from a Pulsar source, compute an aggregate, and upsert
        // that to a JDBC sink.
        //
        // In practice, this should be a different Flink job, independent from the above Postgres -> Flink CDC -> Pulsar
        // work. It's included here mainly for the sake of ease of illustration and experimentation.

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // NOTE: alternatively, instead of registering these tables by hand, external catalogs can be used. See:
        // - Flink & Catalogs: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/common.html#create-tables-in-the-catalog
        // - Pulsar Catalog: https://github.com/streamnative/pulsar-flink#catalog
        // - Postgres Catalog: https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/jdbc.html#postgres-database-as-a-catalog

        // Register table from Pulsar
        tableEnv.executeSql("CREATE TABLE users_from_pulsar (\n" +
                "`key` STRING,\n" +
                "`schema` STRING,\n" +
                "`id` INT,\n" +
                "`fullName` STRING,\n" +
                "`op` STRING,\n" +
                "`eventTime` TIMESTAMP(3) METADATA,\n" +
                "`properties` MAP<STRING, STRING> METADATA ,\n" +
                "`topic` STRING METADATA VIRTUAL,\n" +
                "`sequenceId` BIGINT METADATA VIRTUAL,\n" +
                "PRIMARY KEY (key) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'upsert-pulsar',\n" +
                "'topic' = 'persistent://public/default/users',\n" +
                "'key.format' = 'raw',\n" +
                "'value.format' = 'json',\n" +
                "'service-url' = 'pulsar://localhost:6650',\n" +
                "'admin-url' = 'http://localhost:8080'\n" +
                ")");

        // You can check out the results of a query by converting it to a stream and sinking to stdout
        // https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/common.html#convert-a-table-into-a-datastream
        // Table resultTable = tableEnv.sqlQuery("SELECT * FROM users_from_pulsar");
        // DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(resultTable, Row.class);
        // retractStream.print();

        // Register table from Postgres
        tableEnv.executeSql("CREATE TABLE user_count_by_pgschema (\n" +
                "pgschema STRING,\n" +
                "user_count BIGINT,\n" +
                "PRIMARY KEY (pgschema) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:postgresql://localhost:5434/experiment',\n" +
                "'username' = 'experiment',\n" +
                "'password' = 'experiment',\n" +
                "'table-name' = 'user_count_by_pgschema'\n" +
                ")");
        tableEnv.executeSql("INSERT INTO user_count_by_pgschema (pgschema, user_count) SELECT schema, COUNT(1) as user_count FROM users_from_pulsar GROUP BY schema");

        env.execute("experiment");

        // TODO: demuxing to multiple sinks
        // TODO: test deletes
        // TODO: test source, sink data schema changes
        // TODO: Pulsar eventTime
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
                    break;
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
        Struct source = value.getStruct(FIELD_SOURCE);
        Struct after = value.getStruct(FIELD_AFTER);
        String op = value.getString(FIELD_OP);

        switch (source.getString(FIELD_TABLE)) {
            case StreamingJobWithCustomDeserializer.TABLE_USERS:
                collector.collect(new UsersEvent(
                        op,
                        source.getString(FIELD_SCHEMA),
                        after.getInt64(FIELD_ID),
                        after.getString(FIELD_FULL_NAME)
                ));
                break;
            case StreamingJobWithCustomDeserializer.TABLE_USER_FAVORITE_COLORS:
                collector.collect(new UserFavoriteColorsEvent(
                        op,
                        source.getString(FIELD_SCHEMA),
                        after.getInt64(FIELD_USER_ID),
                        after.getString(FIELD_FAVORITE_COLOR)
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
        super(schema + "|" + id.toString(), op, schema, StreamingJobWithCustomDeserializer.TABLE_USERS);
        this.id = id;
        this.fullName = fullName;
    }

    public String getFullName() {
        return fullName;
    }

    public Long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "UsersEvent{" +
                "key='" + key + '\'' +
                ", fullName='" + fullName + '\'' +
                ", id=" + id +
                ", op='" + op + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                "}";
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
        super(schema + "|" + userId.toString(), op, schema, StreamingJobWithCustomDeserializer.TABLE_USER_FAVORITE_COLORS);
        this.userId = userId;
        this.favoriteColor = favoriteColor;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    public Long getUserId() {
        return userId;
    }

    @Override
    public String toString() {
        return "UserFavoriteColorsEvent{" +
                "key='" + key + '\'' +
                ", favoriteColor='" + favoriteColor + '\'' +
                ", userId=" + userId +
                ", op='" + op + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}

class DebeziumEvent {
    protected String key;
    protected String op;
    protected String schema;
    protected String table;

    public DebeziumEvent(
            String key,
            String op,
            String schema,
            String table
    ) {
        this.key = key;
        this.op = op;
        this.schema = schema;
        this.table = table;
    }

    public String getOp() {
        return op;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "DebeziumEvent{" +
                "key='" + key + '\'' +
                ", op='" + op + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
