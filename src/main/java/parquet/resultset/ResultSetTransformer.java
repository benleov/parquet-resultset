package parquet.resultset;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

/**
 * Transforms an SQL ResultSet into another format.
 */
public interface ResultSetTransformer {

    /**
     * Transforms the result set and writes it to the output stream. The output stream is flushed but not closed,
     * and nothing is held in memory or written to disk other than the current row.
     *
     * @param resultSet    The resultset to transform.
     * @param schemaName   Schema name.
     * @param namespace    Schema namespace.
     * @param listeners    Notified as the schema and each record are parsed. May be null.
     * @param outputStream Where the transformed data is written.
     * @throws IOException
     * @throws SQLException
     */
    void transform(ResultSet resultSet, String schemaName, String namespace, List<TransformerListener> listeners,
                   OutputStream outputStream) throws IOException, SQLException;

    /**
     * Transforms the result set and writes it to the output stream, which is flushed but not closed.
     *
     * @see #transform(ResultSet, String, String, List, OutputStream)
     */
    default void transform(ResultSet resultSet, String schemaName, String namespace,
                           OutputStream outputStream) throws IOException, SQLException {
        transform(resultSet, schemaName, namespace, Collections.emptyList(), outputStream);
    }

    /**
     * Transforms the result set into a temp file and returns a stream to read it, which deletes the file when it is
     * closed. To write somewhere directly without the temp file, use
     * {@link #transform(ResultSet, String, String, List, OutputStream)}.
     *
     * @param resultSet  The resultset to transform.
     * @param schemaName Schema name.
     * @param namespace  Schema namespace.
     * @param listeners  Notified as the schema and each record are parsed. May be null.
     * @return InputStream containing the transformed data.
     * @throws IOException
     * @throws SQLException
     */
    default InputStream transform(ResultSet resultSet, String schemaName, String namespace,
                                  List<TransformerListener> listeners) throws IOException, SQLException {

        Path tempFile = Files.createTempFile("resultset", null);

        try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(tempFile))) {
            transform(resultSet, schemaName, namespace, listeners, outputStream);
        } catch (IOException | SQLException | RuntimeException e) {
            Files.deleteIfExists(tempFile);
            throw e;
        }

        return Files.newInputStream(tempFile, StandardOpenOption.DELETE_ON_CLOSE);
    }

    /**
     * Transforms the result set into a temp file and returns a stream to read it, which deletes the file when it is
     * closed.
     *
     * @see #transform(ResultSet, String, String, List)
     */
    default InputStream transform(ResultSet resultSet, String schemaName, String namespace)
            throws IOException, SQLException {
        return transform(resultSet, schemaName, namespace, (List<TransformerListener>) null);
    }

    /**
     * The avro data model used to write records, with conversions for the logical types used by
     * {@link ResultSetSchemaGenerator}. Use it to read files back with decimals, dates and times as
     * {@link BigDecimal} and java.time values rather than their underlying bytes, ints and longs.
     *
     * @return A new data model.
     */
    static GenericData dataModel() {

        GenericData model = new GenericData();
        model.addLogicalTypeConversion(new Conversions.DecimalConversion());
        model.addLogicalTypeConversion(new TimeConversions.DateConversion());
        model.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        model.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        model.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());

        return model;
    }

    /**
     * Extracts the appropriate value from the ResultSet using the given SchemaSqlMapping. The returned value
     * matches the mapping's field schema, or is null when the column is SQL NULL:
     *
     * <ul>
     *     <li>decimal: {@link BigDecimal}, with the schema's scale</li>
     *     <li>date: {@link java.time.LocalDate}</li>
     *     <li>time-millis: {@link java.time.LocalTime}</li>
     *     <li>local-timestamp-millis: {@link java.time.LocalDateTime}</li>
     *     <li>timestamp-millis: {@link Instant}</li>
     *     <li>bytes: {@link ByteBuffer}</li>
     *     <li>otherwise Boolean, Integer, Long, Float, Double or String</li>
     * </ul>
     *
     * @param mapping
     * @param resultSet
     * @return
     * @throws SQLException
     */
    static Object extractResult(SchemaSqlMapping mapping, ResultSet resultSet) throws SQLException {

        int column = mapping.getColumnIndex();
        Schema fieldSchema = mapping.getFieldSchema();
        LogicalType logicalType = fieldSchema.getLogicalType();
        Object value;

        if (logicalType instanceof LogicalTypes.Decimal) {
            BigDecimal decimal = resultSet.getBigDecimal(column);
            int scale = ((LogicalTypes.Decimal) logicalType).getScale();
            value = decimal != null ? decimal.setScale(scale, RoundingMode.HALF_UP) : null;

        } else if (logicalType instanceof LogicalTypes.Date) {
            java.sql.Date date = resultSet.getDate(column);
            value = date != null ? date.toLocalDate() : null;

        } else if (logicalType instanceof LogicalTypes.TimeMillis) {
            Time time = resultSet.getTime(column);
            // Time#toLocalTime drops the milliseconds, so go through Timestamp to keep them
            value = time != null ? new Timestamp(time.getTime()).toLocalDateTime().toLocalTime() : null;

        } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
            Timestamp timestamp = resultSet.getTimestamp(column);
            value = timestamp != null ? timestamp.toLocalDateTime() : null;

        } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
            value = getInstant(resultSet, column);

        } else {
            switch (fieldSchema.getType()) {
                case BOOLEAN:
                    value = resultSet.getBoolean(column);
                    break;
                case INT:
                    value = resultSet.getInt(column);
                    break;
                case LONG:
                    value = resultSet.getLong(column);
                    break;
                case FLOAT:
                    value = resultSet.getFloat(column);
                    break;
                case DOUBLE:
                    value = resultSet.getDouble(column);
                    break;
                case BYTES:
                    byte[] bytes = resultSet.getBytes(column);
                    value = bytes != null ? ByteBuffer.wrap(bytes) : null;
                    break;
                default:
                    value = resultSet.getString(column);
            }
        }

        // primitive getters return 0 or false for SQL NULL, so check wasNull to keep it null
        return resultSet.wasNull() ? null : value;
    }

    /**
     * Reads a TIMESTAMP_WITH_TIMEZONE as an OffsetDateTime (JDBC 4.2), falling back to getTimestamp for drivers
     * that don't support it.
     */
    private static Instant getInstant(ResultSet resultSet, int column) throws SQLException {

        try {
            OffsetDateTime dateTime = resultSet.getObject(column, OffsetDateTime.class);
            return dateTime != null ? dateTime.toInstant() : null;
        } catch (SQLException e) {
            Timestamp timestamp = resultSet.getTimestamp(column);
            return timestamp != null ? timestamp.toInstant() : null;
        }
    }
}
