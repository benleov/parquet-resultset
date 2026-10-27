package parquet.resultset;

import org.apache.avro.Schema;

/**
 * Mapping between an sql column and schema.
 */
public class SchemaSqlMapping {

    private final int columnIndex;

    private final String schemaName, sqlColumnName;

    private final int sqlType;
    private final Schema fieldSchema;

    /**
     *
     * @param columnIndex The column's index in the ResultSet, starting at 1.
     * @param schemaName The schema field name.
     * @param sqlColumnName The column label (or name if there is no label).
     * @param sqlType {@link java.sql.Types }
     * @param fieldSchema The field's schema, which may have a logical type. Not including the null union.
     */
    public SchemaSqlMapping(int columnIndex, String schemaName, String sqlColumnName, int sqlType,
                            Schema fieldSchema) {
        this.columnIndex = columnIndex;
        this.schemaName = schemaName;
        this.sqlColumnName = sqlColumnName;
        this.sqlType = sqlType;
        this.fieldSchema = fieldSchema;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getSqlColumnName() {
        return sqlColumnName;
    }

    public int getSqlType() {
        return sqlType;
    }

    public Schema getFieldSchema() {
        return fieldSchema;
    }

    public Schema.Type getSchemaType() {
        return fieldSchema.getType();
    }
}
