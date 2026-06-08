package com.example.lakehouse.transform;

import com.example.lakehouse.exception.SchemaNotFoundException;
import com.example.lakehouse.transform.schema.SchemaProvider;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JsonTransformerTest {

    private static final String TOPIC = "test";

    private static final StructType SCHEMA = new StructType()
            .add("id", DataTypes.StringType, false)
            .add("value", DataTypes.IntegerType, true);

    private SparkSession spark;
    private JsonTransformer transformer;

    @BeforeAll
    void setUp() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("JsonTransformerTest")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();

        SchemaProvider provider = topic -> SCHEMA;
        transformer = new JsonTransformer(provider, TOPIC, new SimpleMeterRegistry());
    }

    @AfterAll
    void tearDown() {
        if (spark != null) spark.stop();
    }

    @Test
    void parsesValidJsonIntoValidDataset() {
        Dataset<Row> raw = rawStream(List.of(
                "{\"id\":\"abc\",\"value\":1}",
                "{\"id\":\"def\",\"value\":2}"
        ));

        TransformResult result = transformer.transform(raw);
        List<Row> rows = result.valid().collectAsList();

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getAs("id").toString()).isEqualTo("abc");
    }

    @Test
    void routesMalformedJsonToDeadLetter() {
        Dataset<Row> raw = rawStream(List.of(
                "{\"id\":\"good\",\"value\":1}",
                "not-valid-json"
        ));

        TransformResult result = transformer.transform(raw);

        assertThat(result.valid().collectAsList()).hasSize(1);
        assertThat(result.deadLetter().collectAsList()).hasSize(1);
    }

    @Test
    void appendsMetadataColumnsToValidRows() {
        Dataset<Row> raw = rawStream(List.of("{\"id\":\"x\",\"value\":0}"));

        TransformResult result = transformer.transform(raw);
        Row row = result.valid().collectAsList().get(0);

        assertThat(row.schema().fieldNames()).contains("_ingested_at", "_kafka_offset", "_kafka_partition");
    }

    @Test
    void corruptRecordColumnAbsentInOutputDatasets() {
        Dataset<Row> raw = rawStream(List.of("{\"id\":\"x\",\"value\":0}", "bad"));

        TransformResult result = transformer.transform(raw);

        assertThat(result.valid().schema().fieldNames()).doesNotContain("_corrupt_record");
        assertThat(result.deadLetter().schema().fieldNames()).doesNotContain("_corrupt_record");
    }

    @Test
    void propagatesSchemaNotFoundWhenTopicUnknown() {
        SchemaProvider throwing = topic -> { throw new SchemaNotFoundException(topic); };
        var t = new JsonTransformer(throwing, TOPIC, new SimpleMeterRegistry());

        Dataset<Row> raw = rawStream(List.of("{\"id\":\"x\"}"));
        assertThatThrownBy(() -> t.transform(raw))
                .isInstanceOf(SchemaNotFoundException.class);
    }

    // Builds a micro-batch Dataset from plain JSON strings using a memory stream stub.
    // For unit testing we use a static in-memory dataset backed by a fake Kafka-like schema
    // (value, offset, partition) instead of a real Kafka broker.
    private Dataset<Row> rawStream(List<String> jsonValues) {
        StructType kafkaLikeSchema = new StructType()
                .add("value", DataTypes.BinaryType, true)
                .add("offset", DataTypes.LongType, false)
                .add("partition", DataTypes.IntegerType, false);

        List<Row> rows = new java.util.ArrayList<>();
        for (int i = 0; i < jsonValues.size(); i++) {
            rows.add(org.apache.spark.sql.RowFactory.create(
                    jsonValues.get(i).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    (long) i,
                    0
            ));
        }
        return spark.createDataFrame(rows, kafkaLikeSchema);
    }
}
