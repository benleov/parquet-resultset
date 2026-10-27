import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.List;

/**
 * Creates a {@link ResultSet} over in memory rows that behaves like a JDBC driver: primitive getters return 0 or
 * false for NULL and {@link ResultSet#wasNull()} reports whether the last value read was NULL.
 */
public final class FakeResultSet {

    private FakeResultSet() {
    }

    /**
     * @param metaData    Metadata returned by {@link ResultSet#getMetaData()}.
     * @param columnNames Column names, in the same order as the values in each row.
     * @param rows        Row values, null for SQL NULL.
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

                    int column = columns.indexOf(args[0]);

                    if (column < 0) {
                        throw new IllegalArgumentException("Unknown column " + args[0]);
                    }

                    Object value = rows.get(row[0])[column];
                    wasNull[0] = value == null;

                    switch (method.getName()) {
                        case "getBoolean":
                            return value != null && (Boolean) value;
                        case "getByte":
                            return value == null ? (byte) 0 : ((Number) value).byteValue();
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
                        case "getDate":
                        case "getTime":
                        case "getTimestamp":
                            return value;
                    }

                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
