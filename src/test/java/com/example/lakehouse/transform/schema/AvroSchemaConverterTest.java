package com.example.lakehouse.transform.schema;

import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroSchemaConverterTest {

    @Test
    void convertsStringField() {
        Schema schema = schemaWith("\"string\"");
        StructType result = AvroSchemaConverter.toStructType(schema);
        assertThat(result.apply("f").dataType()).isEqualTo(DataTypes.StringType);
        assertThat(result.apply("f").nullable()).isFalse();
    }

    @Test
    void convertsIntField() {
        StructType result = AvroSchemaConverter.toStructType(schemaWith("\"int\""));
        assertThat(result.apply("f").dataType()).isEqualTo(DataTypes.IntegerType);
    }

    @Test
    void convertsLongField() {
        StructType result = AvroSchemaConverter.toStructType(schemaWith("\"long\""));
        assertThat(result.apply("f").dataType()).isEqualTo(DataTypes.LongType);
    }

    @Test
    void convertsBooleanField() {
        StructType result = AvroSchemaConverter.toStructType(schemaWith("\"boolean\""));
        assertThat(result.apply("f").dataType()).isEqualTo(DataTypes.BooleanType);
    }

    @Test
    void convertsDoubleField() {
        StructType result = AvroSchemaConverter.toStructType(schemaWith("\"double\""));
        assertThat(result.apply("f").dataType()).isEqualTo(DataTypes.DoubleType);
    }

    @Test
    void convertsNullableUnionAsNullable() {
        Schema schema = schemaWith("[\"null\", \"string\"]");
        StructType result = AvroSchemaConverter.toStructType(schema);
        assertThat(result.apply("f").dataType()).isEqualTo(DataTypes.StringType);
        assertThat(result.apply("f").nullable()).isTrue();
    }

    @Test
    void rejectsNonRecordTopLevel() {
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        assertThatThrownBy(() -> AvroSchemaConverter.toStructType(arraySchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RECORD");
    }

    private static Schema schemaWith(String fieldType) {
        return new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"T\",\"fields\":[{\"name\":\"f\",\"type\":" + fieldType + "}]}");
    }
}
