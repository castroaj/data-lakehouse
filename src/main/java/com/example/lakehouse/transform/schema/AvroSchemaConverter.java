package com.example.lakehouse.transform.schema;

import org.apache.avro.Schema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Utility that converts Avro {@link Schema} objects to Spark
 * {@link StructType} for use with {@code from_json()} in the transform layer.
 *
 * <p>Avro nullable unions (e.g. {@code ["null", "string"]}) are mapped to
 * nullable Spark fields. All other unions are unwrapped to their non-null branch.
 *
 * @author Alexander Castro
 */
public final class AvroSchemaConverter {

    private AvroSchemaConverter() {}

    /**
     * Converts a top-level Avro RECORD schema to a Spark StructType.
     *
     * @param avroSchema the Avro schema to convert; must be of type RECORD
     * @return the equivalent Spark StructType with nullability preserved
     * @throws IllegalArgumentException if avroSchema is not a RECORD type
     */
    public static StructType toStructType(Schema avroSchema) {
        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Top-level Avro schema must be RECORD, got: " + avroSchema.getType());
        }
        return buildStructType(avroSchema);
    }

    private static StructType buildStructType(Schema record) {
        List<Schema.Field> fields = record.getFields();
        StructField[] sparkFields = new StructField[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Schema.Field f = fields.get(i);
            boolean nullable = isNullable(f.schema());
            DataType dt = toDataType(unwrapNullable(f.schema()));
            sparkFields[i] = new StructField(f.name(), dt, nullable, Metadata.empty());
        }
        return new StructType(sparkFields);
    }

    private static boolean isNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return false;
        return schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }

    private static Schema unwrapNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return schema;
        return schema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(schema);
    }

    private static DataType toDataType(Schema schema) {
        return switch (schema.getType()) {
            case STRING, ENUM -> DataTypes.StringType;
            case INT -> DataTypes.IntegerType;
            case LONG -> DataTypes.LongType;
            case FLOAT -> DataTypes.FloatType;
            case DOUBLE -> DataTypes.DoubleType;
            case BOOLEAN -> DataTypes.BooleanType;
            case BYTES, FIXED -> DataTypes.BinaryType;
            case NULL -> DataTypes.NullType;
            case RECORD -> buildStructType(schema);
            case ARRAY -> {
                boolean elemNullable = isNullable(schema.getElementType());
                yield new ArrayType(toDataType(unwrapNullable(schema.getElementType())), elemNullable);
            }
            case MAP -> {
                boolean valNullable = isNullable(schema.getValueType());
                yield new MapType(DataTypes.StringType, toDataType(unwrapNullable(schema.getValueType())), valNullable);
            }
            case UNION -> toDataType(unwrapNullable(schema));
        };
    }
}
