import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import parquet.resultset.ResultSetSchemaGenerator;
import parquet.resultset.impl.ResultSetArvoTransformer;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests sql type to schema type mapping, including NULL values, by writing and reading back an avro file.
 */
public class ResultSetTypeMappingTest {

    private static final String[] COLUMN_NAMES = {"id", "big_id", "amount", "end_date", "name"};

    private static final Timestamp END_DATE = Timestamp.valueOf("2017-12-31 00:00:00");

    @Test
    public void testNumericSchemaTypes() {

        ResultSetSchemaGenerator generator = new ResultSetSchemaGenerator();

        assertEquals(Schema.Type.INT, generator.parseSchemaType(Types.NUMERIC, 9, 0));
        assertEquals(Schema.Type.INT, generator.parseSchemaType(Types.DECIMAL, 5, 0));
        assertEquals(Schema.Type.LONG, generator.parseSchemaType(Types.NUMERIC, 10, 0));
        assertEquals(Schema.Type.LONG, generator.parseSchemaType(Types.NUMERIC, 18, 0));
        assertEquals(Schema.Type.DOUBLE, generator.parseSchemaType(Types.NUMERIC, 19, 0));
        assertEquals(Schema.Type.DOUBLE, generator.parseSchemaType(Types.DECIMAL, 23, 5));

        // NUMERIC without precision (e.g. postgres "numeric", oracle "NUMBER") stays a double
        assertEquals(Schema.Type.DOUBLE, generator.parseSchemaType(Types.NUMERIC, 0, 0));
        assertEquals(Schema.Type.DOUBLE, generator.parseSchemaType(Types.NUMERIC, 0, -127));
    }

    @Test
    public void testValuesAndNulls() throws IOException, SQLException {

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1, 10000000000L, new BigDecimal("12.5"), END_DATE, "deposit_1"});
        rows.add(new Object[]{2, null, null, null, null});

        List<GenericRecord> records = transform(rows);

        assertEquals(2, records.size());

        GenericRecord first = records.get(0);
        assertEquals(1, first.get("id"));
        assertEquals(10000000000L, first.get("big_id"));
        assertEquals(12.5, first.get("amount"));
        assertEquals(END_DATE.getTime(), first.get("end_date"));
        assertEquals("deposit_1", first.get("name").toString());

        GenericRecord second = records.get(1);
        assertEquals(2, second.get("id"));
        assertNull(second.get("big_id"));
        assertNull(second.get("amount"));
        assertNull(second.get("end_date"));
        assertNull(second.get("name"));
    }

    private List<GenericRecord> transform(List<Object[]> rows) throws IOException, SQLException {

        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(metaData.getColumnCount()).thenReturn(COLUMN_NAMES.length);

        for (int x = 0; x < COLUMN_NAMES.length; x++) {
            when(metaData.getColumnName(x + 1)).thenReturn(COLUMN_NAMES[x]);
        }

        when(metaData.getColumnType(1)).thenReturn(Types.NUMERIC);   // NUMERIC(9)
        when(metaData.getPrecision(1)).thenReturn(9);
        when(metaData.getColumnType(2)).thenReturn(Types.NUMERIC);   // NUMERIC(18)
        when(metaData.getPrecision(2)).thenReturn(18);
        when(metaData.getColumnType(3)).thenReturn(Types.DECIMAL);   // DECIMAL(23,5)
        when(metaData.getPrecision(3)).thenReturn(23);
        when(metaData.getScale(3)).thenReturn(5);
        when(metaData.getColumnType(4)).thenReturn(Types.TIMESTAMP);
        when(metaData.getColumnType(5)).thenReturn(Types.VARCHAR);

        List<GenericRecord> records = new ArrayList<>();

        try (InputStream inputStream = new ResultSetArvoTransformer().transform(
                createResultSet(metaData, rows), "SchemaName", "org.namespace", new ArrayList<>());
             DataFileStream<GenericRecord> stream = new DataFileStream<>(inputStream, new GenericDatumReader<>())) {

            stream.forEach(records::add);
        }

        return records;
    }

    /**
     * Creates a ResultSet over the given rows that behaves like a JDBC driver: primitive getters return 0
     * for NULL and {@link ResultSet#wasNull()} reports whether the last value read was NULL.
     */
    private static ResultSet createResultSet(ResultSetMetaData metaData, List<Object[]> rows) {

        int[] row = {-1};
        boolean[] wasNull = {false};

        return (ResultSet) Proxy.newProxyInstance(ResultSetTypeMappingTest.class.getClassLoader(),
                new Class[]{ResultSet.class}, (proxy, method, args) -> {

                    switch (method.getName()) {
                        case "getMetaData":
                            return metaData;
                        case "next":
                            return ++row[0] < rows.size();
                        case "wasNull":
                            return wasNull[0];
                    }

                    int column = java.util.Arrays.asList(COLUMN_NAMES).indexOf(args[0]);
                    Object value = rows.get(row[0])[column];
                    wasNull[0] = value == null;

                    switch (method.getName()) {
                        case "getInt":
                            return value == null ? 0 : ((Number) value).intValue();
                        case "getLong":
                            return value == null ? 0L : ((Number) value).longValue();
                        case "getDouble":
                            return value == null ? 0d : ((Number) value).doubleValue();
                        case "getString":
                            return value == null ? null : value.toString();
                        case "getTimestamp":
                            return value;
                    }

                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
