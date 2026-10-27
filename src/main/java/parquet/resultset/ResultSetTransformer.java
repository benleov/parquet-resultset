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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Transforms an SQL ResultSet into another format.
 */
public interface ResultSetTransformer {

    /**
     * Transforms the result set and writes it to the output stream. The output stream is flushed but not closed.
     * Nothing is written to disk, and only the current row (and for parquet, the current row group) is held in
     * memory.
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
     *     <li>array: {@link List} of the element type, which may contain nulls</li>
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
            value = getLocalDate(resultSet, column);

        } else if (logicalType instanceof LogicalTypes.TimeMillis) {
            value = getLocalTime(resultSet, column);

        } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
            value = getLocalDateTime(resultSet, column);

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
                case ARRAY:
                    java.sql.Array array = resultSet.getArray(column);
                    value = array != null ? toList(array, fieldSchema.getElementType(), mapping) : null;
                    break;
                default:
                    value = resultSet.getString(column);
            }
        }

        // primitive getters return 0 or false for SQL NULL, so check wasNull to keep it null
        return resultSet.wasNull() ? null : value;
    }

    /**
     * Converts the elements of an sql array into the element type of the schema, a union of the type and null.
     */
    private static List<Object> toList(java.sql.Array array, Schema elementSchema, SchemaSqlMapping mapping)
            throws SQLException {

        Schema.Type elementType = elementSchema.getTypes().get(0).getType();

        try {
            // drivers may return arrays of primitives, e.g. int[], so read it with reflection
            Object elements = array.getArray();
            int length = java.lang.reflect.Array.getLength(elements);
            List<Object> list = new ArrayList<>(length);

            for (int x = 0; x < length; x++) {
                list.add(toArrayElement(java.lang.reflect.Array.get(elements, x), elementType, mapping));
            }

            return list;

        } finally {
            try {
                array.free();
            } catch (SQLException e) {
                // not supported by all drivers, and the array is released with the result set anyway
            }
        }
    }

    private static Object toArrayElement(Object element, Schema.Type elementType, SchemaSqlMapping mapping)
            throws SQLException {

        if (element == null) {
            return null;
        }

        if (elementType == Schema.Type.STRING) {
            if (element.getClass().isArray()) {
                // an element of a multi dimensional array, e.g. "[1, 2]"
                String nested = Arrays.deepToString(new Object[]{element});
                return nested.substring(1, nested.length() - 1);
            }
            return element.toString();
        }

        if (elementType == Schema.Type.BOOLEAN && element instanceof Boolean) {
            return element;
        }

        if (element instanceof Number) {
            Number number = (Number) element;

            switch (elementType) {
                case INT:
                    return number.intValue();
                case LONG:
                    return number.longValue();
                case FLOAT:
                    return number.floatValue();
                case DOUBLE:
                    return number.doubleValue();
            }
        }

        throw new SQLException("Array element " + element + " (" + element.getClass().getName() + ") in column "
                + mapping.getSqlColumnName() + " cannot be written as " + elementType);
    }

    /*
     * DATE, TIME and TIMESTAMP are read as java.time values (JDBC 4.2), which don't depend on the JVM's time zone.
     * The java.sql types go through it, so e.g. a TIMESTAMP in the hour skipped when daylight saving starts in that
     * zone is moved an hour later. Drivers that don't support getObject(column, Class) fall back to the java.sql
     * getters.
     */

    private static LocalDate getLocalDate(ResultSet resultSet, int column) throws SQLException {

        try {
            return resultSet.getObject(column, LocalDate.class);
        } catch (SQLException e) {
            java.sql.Date date = resultSet.getDate(column);
            return date != null ? date.toLocalDate() : null;
        }
    }

    private static LocalTime getLocalTime(ResultSet resultSet, int column) throws SQLException {

        try {
            return resultSet.getObject(column, LocalTime.class);
        } catch (SQLException e) {
            Time time = resultSet.getTime(column);
            // Time#toLocalTime drops the milliseconds, so go through Timestamp to keep them
            return time != null ? new Timestamp(time.getTime()).toLocalDateTime().toLocalTime() : null;
        }
    }

    private static LocalDateTime getLocalDateTime(ResultSet resultSet, int column) throws SQLException {

        try {
            return resultSet.getObject(column, LocalDateTime.class);
        } catch (SQLException e) {
            Timestamp timestamp = resultSet.getTimestamp(column);
            return timestamp != null ? timestamp.toLocalDateTime() : null;
        }
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
