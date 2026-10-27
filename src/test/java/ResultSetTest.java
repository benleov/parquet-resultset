import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import parquet.resultset.ResultSetTransformer;
import parquet.resultset.SchemaResults;
import parquet.resultset.TransformerListener;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for unit tests.
 */
public abstract class ResultSetTest {

    protected static final String ID_FIELD_NAME = "id";
    protected static final String TEMP_FILE_NAME = "unit_test.tmp";
    protected static final String SCHEMA_NAME = "SchemaName";
    protected static final String NAMESPACE = "org.NAMESPACE";
    protected static final Integer[] ID_VALUES = {0, 1, 2, 3, 4, 5, 6};

    /**
     * Column name, {@link Types} value, precision and scale, and the value written for the first row. Each value
     * is the type the column is expected to be read back as.
     */
    private static final Object[][] TYPED_COLUMNS = {
            {"bool_col", Types.BOOLEAN, 0, 0, true},
            {"tinyint_col", Types.TINYINT, 0, 0, 7},
            {"smallint_col", Types.SMALLINT, 0, 0, 300},
            {"integer_col", Types.INTEGER, 0, 0, 100000},
            {"bigint_col", Types.BIGINT, 0, 0, 10000000000L},
            {"real_col", Types.REAL, 0, 0, 1.5f},
            {"float_col", Types.FLOAT, 0, 0, 2.25d},
            {"double_col", Types.DOUBLE, 0, 0, 3.125d},
            {"numeric9_col", Types.NUMERIC, 9, 0, 123456789},
            {"numeric18_col", Types.NUMERIC, 18, 0, 123456789012L},
            {"decimal_col", Types.DECIMAL, 23, 5, 12.5d},
            {"numeric_col", Types.NUMERIC, 0, 0, 99.75d},
            {"char_col", Types.CHAR, 0, 0, "Y"},
            {"varchar_col", Types.VARCHAR, 0, 0, "deposit_1"},
            {"date_col", Types.DATE, 0, 0, Date.valueOf("2017-12-31")},
            {"time_col", Types.TIME, 0, 0, Time.valueOf("13:45:00")},
            {"timestamp_col", Types.TIMESTAMP, 0, 0, Timestamp.valueOf("2015-12-31 08:30:00")},
    };

    protected ResultSet resultSet = mock(ResultSet.class);
    protected ResultSetMetaData metaData = mock(ResultSetMetaData.class);


    @BeforeEach
    public void before() throws Exception {

        Boolean[] nextReturns = new Boolean[ID_VALUES.length + 1];
        Arrays.fill(nextReturns, Boolean.TRUE);
        nextReturns[nextReturns.length - 1] = false; // set last value to false

        when(resultSet.next()).thenReturn(nextReturns[0], Arrays.copyOfRange(nextReturns, 1, nextReturns.length));
        when(resultSet.getInt(ID_FIELD_NAME)).thenReturn(ID_VALUES[0], Arrays.copyOfRange(ID_VALUES, 1, ID_VALUES.length));
        when(resultSet.getMetaData()).thenReturn(metaData);

        // mock metadata so schema is created
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn(ID_FIELD_NAME);
        when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
    }

    @AfterEach
    public void after() {
        File testOutput = new File(TEMP_FILE_NAME);
        testOutput.delete();
    }

    protected void testListeners(ResultSetTransformer transformer) throws IOException, SQLException {

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

        try(InputStream inputStream = transformer.transform(resultSet, SCHEMA_NAME, NAMESPACE, listeners)) {

            assertTrue(results.schemaParsedCalled);
            assertTrue(results.idFieldParsed);
            assertEquals(1, results.sqlMappingSize);

            assertEquals(Arrays.asList(ID_VALUES), results.parsed);

        }

    }

    protected void testToResultFile(ResultSetTransformer transformer) throws IOException, SQLException {

        List<GenericRecord> records = transformToRecords(transformer, resultSet);

        List<Object> ids = new ArrayList<>();
        records.forEach(record -> ids.add(record.get(ID_FIELD_NAME)));

        assertEquals(Arrays.asList(ID_VALUES), ids);
    }

    /**
     * Writes a row of values and a row of NULLs for each supported sql type, then checks they are read back
     * with the expected schema types and that NULLs stay NULL.
     */
    protected void testTypesAndNulls(ResultSetTransformer transformer) throws IOException, SQLException {

        String[] columnNames = new String[TYPED_COLUMNS.length];
        Object[] values = new Object[TYPED_COLUMNS.length];

        ResultSetMetaData typedMetaData = mock(ResultSetMetaData.class);
        when(typedMetaData.getColumnCount()).thenReturn(TYPED_COLUMNS.length);

        for (int x = 0; x < TYPED_COLUMNS.length; x++) {
            columnNames[x] = (String) TYPED_COLUMNS[x][0];
            values[x] = TYPED_COLUMNS[x][4];

            when(typedMetaData.getColumnName(x + 1)).thenReturn(columnNames[x]);
            when(typedMetaData.getColumnType(x + 1)).thenReturn((Integer) TYPED_COLUMNS[x][1]);
            when(typedMetaData.getPrecision(x + 1)).thenReturn((Integer) TYPED_COLUMNS[x][2]);
            when(typedMetaData.getScale(x + 1)).thenReturn((Integer) TYPED_COLUMNS[x][3]);
        }

        List<Object[]> rows = new ArrayList<>();
        rows.add(values);
        rows.add(new Object[TYPED_COLUMNS.length]);

        List<GenericRecord> records = transformToRecords(transformer,
                FakeResultSet.create(typedMetaData, columnNames, rows));

        assertEquals(2, records.size());

        for (int x = 0; x < TYPED_COLUMNS.length; x++) {

            Object expected = values[x] instanceof java.util.Date ? ((java.util.Date) values[x]).getTime() : values[x];
            Object actual = records.get(0).get(columnNames[x]);

            // avro reads strings back as Utf8
            if (actual instanceof CharSequence) {
                actual = actual.toString();
            }

            assertEquals(expected, actual, columnNames[x]);
            assertNull(records.get(1).get(columnNames[x]), columnNames[x]);
        }
    }

    /**
     * Transforms the result set, writes it to a file and reads the records back from that file.
     */
    private List<GenericRecord> transformToRecords(ResultSetTransformer transformer, ResultSet resultSet)
            throws IOException, SQLException {

        File testOutput = new File(TEMP_FILE_NAME);

        try (InputStream inputStream = transformer.transform(resultSet, SCHEMA_NAME, NAMESPACE, new ArrayList<>());
             OutputStream outputStream = new FileOutputStream(testOutput)) {

            IOUtils.copy(inputStream, outputStream);
        }

        List<GenericRecord> records = readRecords(testOutput);
        assertTrue(!records.isEmpty(), "No records were read from file.");

        return records;
    }

    /**
     * Reads all records from the file produced by the transformer under test.
     */
    public abstract List<GenericRecord> readRecords(File file) throws IOException;

}
