package parquet.resultset;

import org.apache.avro.generic.GenericRecord;

import java.io.File;

/**
 *
 */
public interface TransformerListener {

    /**
     * Called whenever a record parsed.
     *
     * @param record The record that was parsed.
     */
    void onRecordParsed(GenericRecord record);

    /**
     * Called when the schema is parsed from a {@link java.sql.ResultSet }.
     *
     * @param schemaResults The parsed Schema.
     */
    void onSchemaParsed(SchemaResults schemaResults);
}
