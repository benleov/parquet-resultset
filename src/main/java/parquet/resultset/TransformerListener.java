package parquet.resultset;

import org.apache.avro.generic.GenericRecord;

/**
 * Called when a {@link ResultSetTransformer } parses a ResultSet.
 */
public interface TransformerListener {

    /**
     * Called whenever a record is parsed, before it is written.
     *
     * @param record The record that was parsed. Changes to it are written.
     */
    void onRecordParsed(GenericRecord record);

    /**
     * Called when the schema is parsed from a {@link java.sql.ResultSet }.
     *
     * @param schemaResults The parsed Schema.
     */
    void onSchemaParsed(SchemaResults schemaResults);
}
