package parquet.resultset;

import org.apache.avro.Schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ResultSetSchemaGenerator {

    /**
     * Generates parquet schema using {@link ResultSetMetaData }
     *
     * @param resultSet
     * @param name Record name.
     * @param nameSpace
     * @return
     * @throws SQLException
     */
    public SchemaResults generateSchema(ResultSet resultSet, String name, String nameSpace) throws SQLException {

        SchemaResults schemaResults = new SchemaResults();
        List<SchemaSqlMapping> mappings = new ArrayList<>();

        Schema recordSchema = Schema.createRecord(name, null, nameSpace, false);

        List<Schema.Field> fields = new ArrayList<>();

        if (resultSet != null) {

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();

            for (int x = 1; x <= columnCount; x++) {

                String columnName = resultSetMetaData.getColumnName(x);
                int sqlColumnType = resultSetMetaData.getColumnType(x);
                String schemaName = columnName.toLowerCase();

                Schema.Type schemaType = parseSchemaType(sqlColumnType);
                mappings.add(new SchemaSqlMapping(schemaName, columnName, sqlColumnType, schemaType));
                fields.add(createNullableField(recordSchema, schemaName, schemaType));
            }
        }

        recordSchema.setFields(fields);

        schemaResults.setMappings(mappings);
        schemaResults.setParsedSchema(recordSchema);

        return schemaResults;
    }

    /**
     * Converts sql column type to schema type. See {@link java.sql.Types } and {@link Schema.Type } for more details.
     *
     * @param sqlColumnType
     * @return
     */
    public Schema.Type parseSchemaType(int sqlColumnType) {
        switch (sqlColumnType) {

            case Types.BOOLEAN:
                return Schema.Type.BOOLEAN;

            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.ROWID:
                return Schema.Type.INT;

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                return Schema.Type.STRING;

            case Types.FLOAT:
                return Schema.Type.FLOAT;

            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return Schema.Type.DOUBLE;

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return Schema.Type.LONG;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.REF:
            case Types.DATALINK:
            case Types.NCLOB:
            case Types.REF_CURSOR:
                return Schema.Type.BYTES;
        }

        return null;
    }


    /**
     * Creates field that can be null {@link Schema#createUnion(List)}
     *
     * @param recordSchema The root record schema
     * @param columnName   The schema column name
     * @param type         Schema type for this field
     * @return
     */
    public Schema.Field createNullableField(Schema recordSchema, String columnName, Schema.Type type) {

        Schema intSchema = Schema.create(type);
        Schema nullSchema = Schema.create(Schema.Type.NULL);

        List<Schema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(intSchema);
        fieldSchemas.add(nullSchema);

        Schema fieldSchema = recordSchema.createUnion(fieldSchemas);

        return new Schema.Field(columnName, fieldSchema, null, null);
    }
}