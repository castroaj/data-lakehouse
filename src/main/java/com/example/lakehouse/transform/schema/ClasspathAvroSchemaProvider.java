package com.example.lakehouse.transform.schema;

import com.example.lakehouse.exception.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads Avro schemas from {@code /schemas/{topic}.avsc} on the classpath at
 * construction time, converting them to Spark {@link StructType}.
 *
 * <p>Pass the set of expected topics at construction so missing schema files
 * are caught at startup rather than mid-stream when a batch is processing.
 *
 * @author Alexander Castro
 * @see AvroSchemaConverter
 */
public class ClasspathAvroSchemaProvider implements SchemaProvider {

    private static final Logger log = LoggerFactory.getLogger(ClasspathAvroSchemaProvider.class);

    /** Pre-loaded schemas keyed by topic name; populated once at construction. */
    private final Map<String, StructType> schemas;

    /**
     * Loads and converts the Avro schema for each topic at construction time.
     * Throws immediately if any schema file is missing or unparseable so failures
     * surface before the Spark session starts.
     *
     * @param topics the Kafka topic names whose schemas must be present on the classpath; must not be empty
     * @throws SchemaNotFoundException if a {@code .avsc} file for any topic is absent or cannot be read
     */
    public ClasspathAvroSchemaProvider(Collection<String> topics) {
        schemas = new HashMap<>(topics.size());
        for (String topic : topics) {
            String path = "/schemas/" + topic + ".avsc";
            try (InputStream is = ClasspathAvroSchemaProvider.class.getResourceAsStream(path)) {
                if (is == null) {
                    throw new SchemaNotFoundException(topic);
                }
                Schema avroSchema = new Schema.Parser().parse(is);
                schemas.put(topic, AvroSchemaConverter.toStructType(avroSchema));
                log.info("schema loaded topic={} path={}", topic, path);
            } catch (IOException e) {
                throw new SchemaNotFoundException(topic);
            }
        }
    }

    /**
     * Returns the pre-loaded Spark StructType for the given Kafka topic.
     *
     * @param topic the Kafka topic name; must not be blank
     * @return the pre-loaded StructType; never null
     * @throws SchemaNotFoundException if no schema was loaded for the topic
     */
    @Override
    public StructType schemaFor(String topic) {
        StructType schema = schemas.get(topic);
        if (schema == null) {
            throw new SchemaNotFoundException(topic);
        }
        return schema;
    }
}
