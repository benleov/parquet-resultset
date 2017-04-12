package parquet.resultset;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Transforms an SQL resultset into Parquet.
 */
public interface ResultSetTransformer {

    /**
     *
     * @param resultSet The resultset to transform.
     * @param schemaName Schema name.
     * @param namespace Schema namespace.
     * @param listeners
     * @return
     * @throws IOException
     * @throws SQLException
     */
    InputStream toParquet(ResultSet resultSet, String schemaName, String namespace,
                          List<TransformerListener> listeners) throws IOException, SQLException;


    /**
     * Maps SQL types to Schema types.
     *
     * @param mapping
     * @param resultSet
     * @return
     * @throws SQLException
     */
    static Object extractResult(SchemaSqlMapping mapping, ResultSet resultSet) throws SQLException {

        switch (mapping.getSqlType()) {
            case Types.BOOLEAN:
                return resultSet.getBoolean(mapping.getSqlColumnName());
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.ROWID:
                return resultSet.getInt(mapping.getSqlColumnName());
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                return resultSet.getString(mapping.getSqlColumnName());
            case Types.REAL:
            case Types.FLOAT:
                return resultSet.getFloat(mapping.getSqlColumnName());
            case Types.DOUBLE:
                return resultSet.getDouble(mapping.getSqlColumnName());
            case Types.NUMERIC:
                return resultSet.getBigDecimal(mapping.getSqlColumnName());
            case Types.DECIMAL:
                return resultSet.getBigDecimal(mapping.getSqlColumnName());
            case Types.DATE:
                return resultSet.getDate(mapping.getSqlColumnName()).getTime();
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return resultSet.getTime(mapping.getSqlColumnName()).getTime();
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return resultSet.getTimestamp(mapping.getSqlColumnName()).getTime();
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
                return resultSet.getByte(mapping.getSqlColumnName());
            default:
                return resultSet.getString(mapping.getSqlColumnName());
        }
    }
}
