import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import parquet.resultset.ResultSetTransformer;
import parquet.resultset.SchemaResults;
import parquet.resultset.TransformerListener;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for transformer unit tests. Each test runs for every transformer, reading the records back from the
 * transformed output with {@link #readRecords(File)}.
 */
public abstract class ResultSetTest {

    protected static final String ID_FIELD_NAME = "id";
    protected static final String TEMP_FILE_NAME = "unit_test.tmp";
    protected static final String SCHEMA_NAME = "SchemaName";
    protected static final String NAMESPACE = "org.NAMESPACE";
    protected static final Integer[] ID_VALUES = {0, 1, 2, 3, 4, 5, 6};

    /**
     * Column label, {@link Types} value, precision, scale, the value the driver returns, the value expected
     * to be read back, and optionally the database type name and whether it is signed (default true).
     */
    protected static final Object[][] TYPED_COLUMNS = {
            {"bool_col", Types.BOOLEAN, 0, 0, true, true},
            {"bit_col", Types.BIT, 1, 0, true, true},
            {"bits_col", Types.BIT, 8, 0, new byte[]{5}, ByteBuffer.wrap(new byte[]{5})},
            {"tinyint_col", Types.TINYINT, 0, 0, 7, 7},
            {"smallint_col", Types.SMALLINT, 0, 0, 300, 300},
            {"integer_col", Types.INTEGER, 0, 0, 100000, 100000},
            {"bigint_col", Types.BIGINT, 0, 0, 10000000000L, 10000000000L},
            // e.g. mysql UNSIGNED, where the largest values don't fit in the signed type
            {"utinyint_col", Types.TINYINT, 0, 0, 255, 255, null, false},
            {"uinteger_col", Types.INTEGER, 0, 0, 4294967295L, 4294967295L, null, false},
            {"ubigint_col", Types.BIGINT, 0, 0, new BigDecimal("18446744073709551615"),
                    new BigDecimal("18446744073709551615"), null, false},
            {"real_col", Types.REAL, 0, 0, 1.5f, 1.5f},
            {"float_col", Types.FLOAT, 0, 0, 2.25d, 2.25d},
            {"double_col", Types.DOUBLE, 0, 0, 3.125d, 3.125d},
            {"numeric9_col", Types.NUMERIC, 9, 0, 123456789, 123456789},
            {"numeric18_col", Types.NUMERIC, 18, 0, 123456789012L, 123456789012L},
            {"numeric30_col", Types.NUMERIC, 30, 0, new BigDecimal("123456789012345678901234567890"),
                    new BigDecimal("123456789012345678901234567890")},
            {"decimal_col", Types.DECIMAL, 23, 5, new BigDecimal("12.5"), new BigDecimal("12.50000")},
            {"numeric_col", Types.NUMERIC, 0, 0, 99.75d, 99.75d},
            {"number_col", Types.NUMERIC, 0, -127, 1.5d, 1.5d},
            {"char_col", Types.CHAR, 0, 0, "Y", "Y"},
            {"varchar_col", Types.VARCHAR, 0, 0, "deposit_1", "deposit_1"},
            {"clob_col", Types.CLOB, 0, 0, "long text", "long text"},
            {"xml_col", Types.SQLXML, 0, 0, "<a/>", "<a/>"},
            {"uuid_col", Types.OTHER, 0, 0, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
            // array element types come from the type name: postgres, sql standard (H2, HSQLDB) and hive styles
            {"int_array_col", Types.ARRAY, 0, 0, new Integer[]{1, null, 3}, Arrays.asList(1, null, 3), "_int4"},
            {"bigint_array_col", Types.ARRAY, 0, 0, new long[]{10000000000L, 2L},
                    Arrays.asList(10000000000L, 2L), "BIGINT ARRAY"},
            {"float_array_col", Types.ARRAY, 0, 0, new Float[]{0.5f}, Arrays.asList(0.5f), "_float4"},
            {"double_array_col", Types.ARRAY, 0, 0, new Double[]{2.5d}, Arrays.asList(2.5d), "array<double>"},
            {"numeric_array_col", Types.ARRAY, 0, 0, new BigDecimal[]{new BigDecimal("1.5")}, Arrays.asList(1.5d),
                    "_numeric"},
            {"bool_array_col", Types.ARRAY, 0, 0, new Boolean[]{true, false}, Arrays.asList(true, false), "_bool"},
            {"text_array_col", Types.ARRAY, 0, 0, new String[]{"a", "b"}, Arrays.asList("a", "b"), "_text"},
            {"date_array_col", Types.ARRAY, 0, 0, new Date[]{Date.valueOf("2020-01-02")},
                    Arrays.asList("2020-01-02"), "_date"},
            {"nested_array_col", Types.ARRAY, 0, 0, new Object[]{new int[]{1, 2}, new String[]{"x"}},
                    Arrays.asList("[1, 2]", "[x]"), null},
            {"unknown_array_col", Types.ARRAY, 0, 0, new Object[]{1, "x"}, Arrays.asList("1", "x"), null},
            {"empty_array_col", Types.ARRAY, 0, 0, new Integer[0], Collections.emptyList(), "_int4"},
            {"varbinary_col", Types.VARBINARY, 0, 0, new byte[]{1, 2, 3}, ByteBuffer.wrap(new byte[]{1, 2, 3})},
            {"blob_col", Types.BLOB, 0, 0, new byte[]{4, 5}, ByteBuffer.wrap(new byte[]{4, 5})},
            {"date_col", Types.DATE, 0, 0, Date.valueOf("2017-12-31"), LocalDate.of(2017, 12, 31)},
            {"time_col", Types.TIME, 0, 0, new Time(Time.valueOf("13:45:30").getTime() + 123),
                    LocalTime.of(13, 45, 30, 123_000_000)},
            {"timestamp_col", Types.TIMESTAMP, 0, 0, Timestamp.valueOf("2015-12-31 08:30:00.123"),
                    LocalDateTime.of(2015, 12, 31, 8, 30, 0, 123_000_000)},
            {"timestamptz_col", Types.TIMESTAMP_WITH_TIMEZONE, 0, 0,
                    OffsetDateTime.of(2015, 12, 31, 8, 30, 0, 0, ZoneOffset.ofHours(10)),
                    Instant.parse("2015-12-30T22:30:00Z")},
            // read with getTimestamp when the driver doesn't support getObject(column, OffsetDateTime.class)
            {"timestamptz_old_col", Types.TIMESTAMP_WITH_TIMEZONE, 0, 0,
                    Timestamp.from(Instant.parse("2020-01-01T00:00:00Z")), Instant.parse("2020-01-01T00:00:00Z")},
            {"null_col", Types.NULL, 0, 0, null, null},
    };

    protected ResultSet resultSet = mock(ResultSet.class);
    protected ResultSetMetaData metaData = mock(ResultSetMetaData.class);

    /**
     * @return The transformer under test.
     */
    protected abstract ResultSetTransformer createTransformer();

    /**
     * Reads all records from the file produced by the transformer under test, using
     * {@link ResultSetTransformer#dataModel()} so logical types are read as java types.
     */
    public abstract List<GenericRecord> readRecords(File file) throws IOException;

    @BeforeEach
    public void before() throws Exception {

        Boolean[] nextReturns = new Boolean[ID_VALUES.length + 1];
        Arrays.fill(nextReturns, Boolean.TRUE);
        nextReturns[nextReturns.length - 1] = false; // set last value to false

        when(resultSet.next()).thenReturn(nextReturns[0], Arrays.copyOfRange(nextReturns, 1, nextReturns.length));
        when(resultSet.getInt(1)).thenReturn(ID_VALUES[0], Arrays.copyOfRange(ID_VALUES, 1, ID_VALUES.length));
        when(resultSet.getMetaData()).thenReturn(metaData);

        // mock metadata so schema is created
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn(ID_FIELD_NAME);
        when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
        when(metaData.isSigned(1)).thenReturn(true);
    }

    @AfterEach
    public void after() {
        File testOutput = new File(TEMP_FILE_NAME);
        testOutput.delete();
    }

    @Test
    public void testListeners() throws IOException, SQLException {

        List<TransformerListener> listeners = new ArrayList<>();

        class Results {
            List<Integer> parsed = new ArrayList<>();
            boolean schemaParsedCalled = false;
            boolean idFieldParsed = false;
            int sqlMappingSize;
        }

        final Results results = new Results();

        listeners.add(new TransformerListener() {
            @Override
            public void onRecordParsed(GenericRecord record) {
                results.parsed.add((Integer)record.get(ID_FIELD_NAME));
            }

            @Override
            public void onSchemaParsed(SchemaResults schemaResults) {

                results.schemaParsedCalled = true;
                results.idFieldParsed = schemaResults.getParsedSchema().getField(ID_FIELD_NAME) != null;
                results.sqlMappingSize = schemaResults.getMappings().size();
            }
        });

        try(InputStream inputStream = createTransformer().transform(resultSet, SCHEMA_NAME, NAMESPACE, listeners)) {

            assertTrue(results.schemaParsedCalled);
            assertTrue(results.idFieldParsed);
            assertEquals(1, results.sqlMappingSize);

            assertEquals(Arrays.asList(ID_VALUES), results.parsed);

        }

    }

    @Test
    public void testToFile() throws IOException, SQLException {

        List<GenericRecord> records = toRecords(createTransformer().transform(resultSet, SCHEMA_NAME, NAMESPACE));

        assertEquals(Arrays.asList(ID_VALUES), values(records, ID_FIELD_NAME));
    }

    @Test
    public void testNullListeners() throws IOException, SQLException {

        List<GenericRecord> records = toRecords(
                createTransformer().transform(resultSet, SCHEMA_NAME, NAMESPACE, (List<TransformerListener>) null));

        assertEquals(Arrays.asList(ID_VALUES), values(records, ID_FIELD_NAME));
    }

    @Test
    public void testToOutputStream() throws IOException, SQLException {

        boolean[] closed = {false};

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream() {
            @Override
            public void close() {
                closed[0] = true;
            }
        };

        createTransformer().transform(resultSet, SCHEMA_NAME, NAMESPACE, outputStream);

        assertFalse(closed[0], "The caller's output stream should not be closed.");

        File testOutput = new File(TEMP_FILE_NAME);
        Files.write(testOutput.toPath(), outputStream.toByteArray());

        assertEquals(Arrays.asList(ID_VALUES), values(readRecords(testOutput), ID_FIELD_NAME));
    }

    @Test
    public void testEmptyResultSet() throws IOException, SQLException {

        when(resultSet.next()).thenReturn(false);

        assertEquals(0, toRecords(createTransformer().transform(resultSet, SCHEMA_NAME, NAMESPACE)).size());
    }

    /**
     * Writes a row of values and a row of NULLs for each supported sql type, then checks they are read back
     * with the expected schema types and that NULLs stay NULL.
     */
    @Test
    public void testTypesAndNulls() throws IOException, SQLException {

        List<GenericRecord> records = toRecords(createTransformer().transform(createTypedResultSet(),
                SCHEMA_NAME, NAMESPACE));

        assertTypedRecords(records);
    }

    /**
     * Columns are read by index, so a join's columns with the same name keep their own values, and names that
     * are not valid field names are changed rather than failing.
     */
    @Test
    public void testDuplicateAndInvalidColumnNames() throws IOException, SQLException {

        String[] labels = {"ID", "id", "COUNT(*)", "1st", "first name"};

        ResultSetMetaData namesMetaData = mock(ResultSetMetaData.class);
        when(namesMetaData.getColumnCount()).thenReturn(labels.length);

        for (int x = 0; x < labels.length; x++) {
            when(namesMetaData.getColumnLabel(x + 1)).thenReturn(labels[x]);
            when(namesMetaData.getColumnType(x + 1)).thenReturn(x < 3 ? Types.INTEGER : Types.VARCHAR);
            when(namesMetaData.isSigned(x + 1)).thenReturn(true);
        }

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1, 2, 3, "a", "b"});

        GenericRecord record = toRecords(createTransformer().transform(
                FakeResultSet.create(namesMetaData, labels, rows), SCHEMA_NAME, NAMESPACE)).get(0);

        assertEquals(1, record.get("id"));
        assertEquals(2, record.get("id_2"));
        assertEquals(3, record.get("count___"));
        assertEquals("a", record.get("_1st").toString());
        assertEquals("b", record.get("first_name").toString());
    }

    /**
     * A multi dimensional array of a non string type, e.g. a postgres int[][] which has the same type name as
     * int[], can't be written as the element type and fails with a clear error.
     */
    @Test
    public void testArrayOfArraysOfNumbersFails() throws SQLException {

        String[] labels = {"matrix"};

        ResultSetMetaData arrayMetaData = mock(ResultSetMetaData.class);
        when(arrayMetaData.getColumnCount()).thenReturn(1);
        when(arrayMetaData.getColumnLabel(1)).thenReturn(labels[0]);
        when(arrayMetaData.getColumnType(1)).thenReturn(Types.ARRAY);
        when(arrayMetaData.getColumnTypeName(1)).thenReturn("_int4");

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{new Integer[][]{{1, 2}, {3, 4}}});

        SQLException exception = assertThrows(SQLException.class, () -> createTransformer().transform(
                FakeResultSet.create(arrayMetaData, labels, rows), SCHEMA_NAME, NAMESPACE));

        assertTrue(exception.getMessage().contains("matrix"), exception.getMessage());
    }

    protected static ResultSet createTypedResultSet() throws SQLException {

        String[] columnNames = new String[TYPED_COLUMNS.length];
        Object[] values = new Object[TYPED_COLUMNS.length];

        ResultSetMetaData typedMetaData = mock(ResultSetMetaData.class);
        when(typedMetaData.getColumnCount()).thenReturn(TYPED_COLUMNS.length);

        for (int x = 0; x < TYPED_COLUMNS.length; x++) {
            columnNames[x] = (String) TYPED_COLUMNS[x][0];
            values[x] = TYPED_COLUMNS[x][4];

            when(typedMetaData.getColumnLabel(x + 1)).thenReturn(columnNames[x]);
            when(typedMetaData.getColumnType(x + 1)).thenReturn((Integer) TYPED_COLUMNS[x][1]);
            when(typedMetaData.getPrecision(x + 1)).thenReturn((Integer) TYPED_COLUMNS[x][2]);
            when(typedMetaData.getScale(x + 1)).thenReturn((Integer) TYPED_COLUMNS[x][3]);
            when(typedMetaData.getColumnTypeName(x + 1))
                    .thenReturn(TYPED_COLUMNS[x].length > 6 ? (String) TYPED_COLUMNS[x][6] : null);
            when(typedMetaData.isSigned(x + 1))
                    .thenReturn(TYPED_COLUMNS[x].length > 7 ? (Boolean) TYPED_COLUMNS[x][7] : true);
        }

        List<Object[]> rows = new ArrayList<>();
        rows.add(values);
        rows.add(new Object[TYPED_COLUMNS.length]);

        return FakeResultSet.create(typedMetaData, columnNames, rows);
    }

    protected static void assertTypedRecords(List<GenericRecord> records) {

        assertEquals(2, records.size());

        for (Object[] column : TYPED_COLUMNS) {

            String name = (String) column[0];

            assertEquals(column[5], normalise(records.get(0).get(name)), name);
            assertNull(records.get(1).get(name), name);
        }
    }

    /**
     * Avro reads strings back as Utf8, including in arrays, so convert them to Strings.
     */
    private static Object normalise(Object value) {

        if (value instanceof CharSequence) {
            return value.toString();
        }

        if (value instanceof List) {
            List<Object> list = new ArrayList<>();
            ((List<?>) value).forEach(element -> list.add(normalise(element)));
            return list;
        }

        return value;
    }

    /**
     * Writes the transformed stream to a file, closing the stream, and reads the records back from the file.
     */
    protected List<GenericRecord> toRecords(InputStream transformed) throws IOException {

        File testOutput = new File(TEMP_FILE_NAME);

        try (InputStream inputStream = transformed;
             OutputStream outputStream = new FileOutputStream(testOutput)) {

            IOUtils.copy(inputStream, outputStream);
        }

        return readRecords(testOutput);
    }

    private static List<Object> values(List<GenericRecord> records, String fieldName) {

        List<Object> values = new ArrayList<>();
        records.forEach(record -> values.add(record.get(fieldName)));

        return values;
    }
}
