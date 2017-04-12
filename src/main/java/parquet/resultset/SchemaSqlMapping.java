package parquet.resultset;

import org.apache.avro.Schema;

/**
 * Mapping between an sql column and schema.
 */
public class SchemaSqlMapping {

    private final String schemaName, sqlColumnName;

    private final int sqlType;
    private final Schema.Type schemaType;

    /**
     *
     * @param schemaName
     * @param sqlColumnName
     * @param sqlType {@link java.sql.Types }
     * @param schemaType
     */
    public SchemaSqlMapping(String schemaName, String sqlColumnName, int sqlType, Schema.Type schemaType) {
        this.schemaName = schemaName;
        this.sqlColumnName = sqlColumnName;
        this.sqlType = sqlType;
        this.schemaType = schemaType;
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

    public Schema.Type getSchemaType() {
        return schemaType;
    }
}