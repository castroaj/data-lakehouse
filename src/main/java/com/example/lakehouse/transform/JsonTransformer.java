package com.example.lakehouse.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class JsonTransformer {

    public record TransformResult(Dataset<Row> valid, Dataset<Row> deadLetter) {}

    public TransformResult transform(Dataset<Row> raw, StructType schema) {
        // TODO: from_json parse → split on _corrupt_record → append _ingested_at, _kafka_offset, _kafka_partition
        throw new UnsupportedOperationException("not implemented");
    }
}
