package parquet.resultset.impl;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import parquet.resultset.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Generates the schema, converts each row into a record and notifies listeners. Subclasses write the records.
 */
public abstract class AbstractResultSetTransformer implements ResultSetTransformer {

    @Override
    public void transform(ResultSet resultSet, String schemaName, String namespace,
                          List<TransformerListener> listeners, OutputStream outputStream)
            throws IOException, SQLException {

        List<TransformerListener> notify = listeners != null ? listeners : Collections.emptyList();

        SchemaResults schemaResults = new ResultSetSchemaGenerator().generateSchema(resultSet,
                schemaName, namespace);

        notify.forEach(transformerListener -> transformerListener.onSchemaParsed(schemaResults));

        Schema schema = schemaResults.getParsedSchema();
        List<SchemaSqlMapping> mappings = schemaResults.getMappings();

        try (RecordWriter writer = createWriter(schema, ResultSetTransformer.dataModel(),
                new NonClosingOutputStream(outputStream))) {

            while (resultSet != null && resultSet.next()) {

                // fields are in the same order as the mappings
                GenericRecord record = new GenericData.Record(schema);

                for (int x = 0; x < mappings.size(); x++) {
                    record.put(x, ResultSetTransformer.extractResult(mappings.get(x), resultSet));
                }

                notify.forEach(transformerListener -> transformerListener.onRecordParsed(record));

                writer.write(record);
            }
        }
    }

    /**
     * Creates the writer for the records.
     *
     * @param schema       The record schema.
     * @param dataModel    Data model with the logical type conversions for the record values.
     * @param outputStream Where to write. Closing it only flushes it.
     * @return
     * @throws IOException
     */
    protected abstract RecordWriter createWriter(Schema schema, GenericData dataModel, OutputStream outputStream)
            throws IOException;

    /**
     * Writes records in the transformed format. Closing it finishes the output.
     */
    protected interface RecordWriter extends Closeable {

        void write(GenericRecord record) throws IOException;
    }
}
