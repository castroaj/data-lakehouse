package com.example.lakehouse.transform;

import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class SchemaRegistry {

    public Optional<StructType> schemaFor(String topic) {
        // TODO: return registered StructType for the topic; throw SchemaNotFoundException if absent
        throw new UnsupportedOperationException("not implemented");
    }
}
