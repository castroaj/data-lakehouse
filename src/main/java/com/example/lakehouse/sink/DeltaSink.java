package com.example.lakehouse.sink;

import com.example.lakehouse.config.IngestionConfig;
import com.example.lakehouse.transform.JsonTransformer.TransformResult;
import org.apache.spark.sql.streaming.StreamingQuery;

public class DeltaSink {

    public StreamingQuery write(TransformResult result, IngestionConfig config) {
        // TODO: writeStream valid rows to Delta table; writeStream dead-letter rows to JSON path
        throw new UnsupportedOperationException("not implemented");
    }
}
