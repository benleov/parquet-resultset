package parquet.resultset;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Transforms an SQL ResultSet into another format.
 */
public interface ResultSetTransformer {

    /**
     * @param resultSet  The resultset to transform.
     * @param schemaName Schema name.
     * @param namespace  Schema namespace.
     * @param listeners
     * @return InputStream containing the transformed data.
     * @throws IOException
     * @throws SQLException
     */
    InputStream transform(ResultSet resultSet, String schemaName, String namespace,
                          List<TransformerListener> listeners) throws IOException, SQLException;


    /**
     * Extracts the appropriate value from the ResultSet using the given SchemaSqlMapping. The returned value
     * matches the mapping's schema type, or is null when the column is SQL NULL.
     *
     * @param mapping
     * @param resultSet
     * @return
     * @throws SQLException
     */
    static Object extractResult(SchemaSqlMapping mapping, ResultSet resultSet) throws SQLException {

        String columnName = mapping.getSqlColumnName();
        Object value;

        switch (mapping.getSqlType()) {
            case Types.BOOLEAN:
                value = resultSet.getBoolean(columnName);
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                value = resultSet.getInt(columnName);
                break;
            case Types.BIGINT:
                value = resultSet.getLong(columnName);
                break;
            case Types.ROWID:
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                value = resultSet.getString(columnName);
                break;
            case Types.REAL:
                value = resultSet.getFloat(columnName);
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                value = resultSet.getDouble(columnName);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                // read using the type chosen by ResultSetSchemaGenerator#parseSchemaType(int, int, int)
                switch (mapping.getSchemaType()) {
                    case INT:
                        value = resultSet.getInt(columnName);
                        break;
                    case LONG:
                        value = resultSet.getLong(columnName);
                        break;
                    default:
                        value = resultSet.getDouble(columnName);
                }
                break;
            case Types.DATE:
                java.sql.Date date = resultSet.getDate(columnName);
                value = date != null ? date.getTime() : null;
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                java.sql.Time time = resultSet.getTime(columnName);
                value = time != null ? time.getTime() : null;
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                java.sql.Timestamp timestamp = resultSet.getTimestamp(columnName);
                value = timestamp != null ? timestamp.getTime() : null;
                break;
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
                value = resultSet.getByte(columnName);
                break;
            default:
                value = resultSet.getString(columnName);
        }

        // primitive getters return 0 or false for SQL NULL, so check wasNull to keep it null
        return resultSet.wasNull() ? null : value;
    }
}
