package com.example.lakehouse.transform.schema;

import com.example.lakehouse.exception.SchemaNotFoundException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClasspathAvroSchemaProviderTest {

    @Test
    void loadsSchemaFromClasspath() {
        var provider = new ClasspathAvroSchemaProvider(List.of("test-topic"));
        StructType schema = provider.schemaFor("test-topic");

        assertThat(schema.fieldNames()).containsExactly("id", "count", "score", "active", "optional");
        assertThat(schema.apply("id").dataType()).isEqualTo(DataTypes.StringType);
        assertThat(schema.apply("count").dataType()).isEqualTo(DataTypes.IntegerType);
        assertThat(schema.apply("score").dataType()).isEqualTo(DataTypes.DoubleType);
        assertThat(schema.apply("active").dataType()).isEqualTo(DataTypes.BooleanType);
        assertThat(schema.apply("optional").nullable()).isTrue();
    }

    @Test
    void throwsSchemaNotFoundForMissingFile() {
        assertThatThrownBy(() -> new ClasspathAvroSchemaProvider(List.of("no-such-topic")))
                .isInstanceOf(SchemaNotFoundException.class)
                .hasMessageContaining("no-such-topic");
    }

    @Test
    void throwsSchemaNotFoundForUnregisteredTopicOnLookup() {
        var provider = new ClasspathAvroSchemaProvider(List.of("test-topic"));
        assertThatThrownBy(() -> provider.schemaFor("other-topic"))
                .isInstanceOf(SchemaNotFoundException.class)
                .hasMessageContaining("other-topic");
    }
}
