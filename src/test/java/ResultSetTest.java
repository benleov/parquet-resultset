import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import parquet.resultset.ResultSetTransformer;
import parquet.resultset.SchemaResults;
import parquet.resultset.TransformerListener;
import parquet.resultset.impl.ResultSetArvoTransformer;
import parquet.resultset.impl.ResultSetParquetTransformer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
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

    protected ResultSet resultSet = mock(ResultSet.class);
    protected ResultSetMetaData metaData = mock(ResultSetMetaData.class);


    @Before
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

    @After
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

        List<TransformerListener> listeners = new ArrayList<>();

        InputStream inputStream = transformer.transform(resultSet, SCHEMA_NAME, NAMESPACE, listeners);

        File testOutput = new File(TEMP_FILE_NAME);

        IOUtils.copy(inputStream, new FileOutputStream(testOutput));
        validate(testOutput, ID_FIELD_NAME, ID_VALUES);
    }



    public abstract void validate(File file, String fieldName, Integer... expectedValues) throws IOException;

}
