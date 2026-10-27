import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Creates a {@link ResultSet} over in memory rows that behaves like a JDBC driver: primitive getters return 0 or
 * false for NULL and {@link ResultSet#wasNull()} reports whether the last value read was NULL. Columns can be read
 * by index or by label.
 */
public final class FakeResultSet {

    private FakeResultSet() {
    }

    /**
     * @param metaData    Metadata returned by {@link ResultSet#getMetaData()}.
     * @param columnNames Column labels, in the same order as the values in each row.
     * @param rows        Row values, null for SQL NULL. getObject(column, Class) returns values of that class
     *                    and is otherwise unsupported, as with older drivers, e.g. a {@link java.sql.Date} can only
     *                    be read with getDate.
     */
    public static ResultSet create(ResultSetMetaData metaData, String[] columnNames, List<Object[]> rows) {

        List<String> columns = Arrays.asList(columnNames);
        int[] row = {-1};
        boolean[] wasNull = {false};

        return (ResultSet) Proxy.newProxyInstance(FakeResultSet.class.getClassLoader(),
                new Class[]{ResultSet.class}, (proxy, method, args) -> {

                    switch (method.getName()) {
                        case "getMetaData":
                            return metaData;
                        case "next":
                            return ++row[0] < rows.size();
                        case "wasNull":
                            return wasNull[0];
                        case "close":
                            return null;
                    }

                    int column = args[0] instanceof Integer ? (Integer) args[0] - 1 : columns.indexOf(args[0]);

                    if (column < 0 || column >= columns.size()) {
                        throw new IllegalArgumentException("Unknown column " + args[0]);
                    }

                    Object value = rows.get(row[0])[column];
                    wasNull[0] = value == null;

                    switch (method.getName()) {
                        case "getBoolean":
                            return value != null && (Boolean) value;
                        case "getInt":
                            return value == null ? 0 : ((Number) value).intValue();
                        case "getLong":
                            return value == null ? 0L : ((Number) value).longValue();
                        case "getFloat":
                            return value == null ? 0f : ((Number) value).floatValue();
                        case "getDouble":
                            return value == null ? 0d : ((Number) value).doubleValue();
                        case "getBigDecimal":
                            return value == null ? null : new BigDecimal(value.toString());
                        case "getString":
                            return value == null ? null : value.toString();
                        case "getBytes":
                        case "getDate":
                        case "getTime":
                            return value;
                        case "getArray":
                            return value == null ? null : createArray(value);
                        case "getTimestamp":
                            return value instanceof OffsetDateTime
                                    ? Timestamp.from(((OffsetDateTime) value).toInstant()) : value;
                        case "getObject":
                            if (value != null && !((Class<?>) args[1]).isInstance(value)) {
                                throw new SQLFeatureNotSupportedException("getObject(int, Class)");
                            }
                            return value;
                    }

                    throw new UnsupportedOperationException(method.getName());
                });
    }

    /**
     * @param elements A java array, which may be an array of primitives, returned by {@link Array#getArray()}.
     */
    private static Array createArray(Object elements) {

        return (Array) Proxy.newProxyInstance(FakeResultSet.class.getClassLoader(), new Class[]{Array.class},
                (proxy, method, args) -> {

                    switch (method.getName()) {
                        case "getArray":
                            return elements;
                        case "free":
                            // some drivers don't support free
                            throw new SQLFeatureNotSupportedException("free");
                    }

                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
